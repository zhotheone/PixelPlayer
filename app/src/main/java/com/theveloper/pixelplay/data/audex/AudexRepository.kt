package com.theveloper.pixelplay.data.audex

import android.content.Context
import android.content.SharedPreferences
import androidx.core.content.FileProvider
import androidx.core.content.edit
import androidx.security.crypto.EncryptedSharedPreferences
import androidx.security.crypto.MasterKey
import com.theveloper.pixelplay.data.audex.model.AudexCredentials
import com.theveloper.pixelplay.data.audex.model.AudexInfoResponse
import com.theveloper.pixelplay.data.audex.model.AudexLibraryResponse
import com.theveloper.pixelplay.data.database.AlbumEntity
import com.theveloper.pixelplay.data.database.ArtistEntity
import com.theveloper.pixelplay.data.database.MusicDao
import com.theveloper.pixelplay.data.database.SongArtistCrossRef
import com.theveloper.pixelplay.data.database.SongEntity
import com.theveloper.pixelplay.data.database.SourceType
import com.theveloper.pixelplay.data.database.toSong
import com.theveloper.pixelplay.data.model.Song
import dagger.hilt.android.qualifiers.ApplicationContext
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import kotlinx.coroutines.withContext
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.json.Json
import okhttp3.OkHttpClient
import okhttp3.Request
import timber.log.Timber
import java.io.File
import java.net.ConnectException
import java.net.SocketTimeoutException
import java.net.UnknownHostException
import java.util.concurrent.TimeUnit
import javax.inject.Inject
import javax.inject.Singleton
import kotlin.math.absoluteValue

/**
 * Client for an Audex desktop instance's LAN sharing API (plain HTTP+JSON,
 * bearer-token auth — see audex-player's `lan.js`). Unlike Navidrome/Jellyfin,
 * Audex's whole library arrives in one `/api/library` call and its media URLs
 * are permanently HMAC-signed (no per-request token regen needed), so there's
 * no intermediate Room table and no stream proxy: synced tracks go straight
 * into the unified `songs` table with the real signed URL as
 * `contentUriString`, and cover art is downloaded once to local storage
 * (survives the source device going offline/away, unlike relying on Coil's
 * evictable disk cache).
 */
@Singleton
class AudexRepository @Inject constructor(
    private val okHttpClient: OkHttpClient,
    private val musicDao: MusicDao,
    @ApplicationContext private val context: Context,
) {
    companion object {
        private const val TAG = "AudexRepo"
        private const val PREFS_NAME = "audex_prefs"
        private const val KEY_HOST = "host"
        private const val KEY_PORT = "port"
        private const val KEY_KEY = "key"
        private const val KEY_NAME = "name"
        private const val COVER_DIR_NAME = "audex_covers"

        // Next free unified-ID range after Netease(3-5)/GDrive+QqMusic(6-8)/Navidrome(9-11)/Jellyfin(12-14).
        private const val AUDEX_SONG_ID_OFFSET = 15_000_000_000_000L
        private const val AUDEX_ALBUM_ID_OFFSET = 16_000_000_000_000L
        private const val AUDEX_ARTIST_ID_OFFSET = 17_000_000_000_000L

        // A LAN peer either answers in well under a second or it's unreachable —
        // no reason to inherit the shared client's 8s default, which turns a
        // single stalled request into a long wait and, multiplied across a
        // library's worth of sequential cover downloads, into what looks like
        // an endless spinner.
        private const val CONNECT_TIMEOUT_S = 4L
        private const val READ_TIMEOUT_S = 5L
        private const val COVER_FETCH_CONCURRENCY = 6
        // Small enough that each batch's DB commit (and Room Flow re-emit) lands
        // every second or two on a real LAN, so tracks visibly appear in groups
        // rather than either one-by-one or all at once at the very end.
        private const val SYNC_BATCH_SIZE = 25
    }

    private val json = Json { ignoreUnknownKeys = true }

    private val http: OkHttpClient = okHttpClient.newBuilder()
        .connectTimeout(CONNECT_TIMEOUT_S, TimeUnit.SECONDS)
        .readTimeout(READ_TIMEOUT_S, TimeUnit.SECONDS)
        .writeTimeout(CONNECT_TIMEOUT_S, TimeUnit.SECONDS)
        .build()

    private val prefs: SharedPreferences = try {
        val masterKey = MasterKey.Builder(context)
            .setKeyScheme(MasterKey.KeyScheme.AES256_GCM)
            .build()
        EncryptedSharedPreferences.create(
            context,
            PREFS_NAME,
            masterKey,
            EncryptedSharedPreferences.PrefKeyEncryptionScheme.AES256_SIV,
            EncryptedSharedPreferences.PrefValueEncryptionScheme.AES256_GCM
        )
    } catch (e: Exception) {
        Timber.e(e, "$TAG: Failed to create EncryptedSharedPreferences, falling back to plain")
        context.getSharedPreferences("${PREFS_NAME}_plain", Context.MODE_PRIVATE)
    }

    private val _isPairedFlow = MutableStateFlow(readSavedCredentials() != null)
    val isPairedFlow: StateFlow<Boolean> = _isPairedFlow.asStateFlow()
    val isPaired: Boolean get() = _isPairedFlow.value
    val pairedDeviceName: String? get() = prefs.getString(KEY_NAME, null)
    val syncedSongCountFlow: Flow<Int> get() = musicDao.getAudexSongCountFlow()
    /** Reactive — the Library screen and this app's own Audex screen both just observe this. */
    val songsFlow: Flow<List<Song>> get() = musicDao.getAudexSongsFlow().map { it.map(SongEntity::toSong) }

    fun savedCredentials(): AudexCredentials? = readSavedCredentials()

    private fun readSavedCredentials(): AudexCredentials? {
        val host = prefs.getString(KEY_HOST, null) ?: return null
        val port = prefs.getInt(KEY_PORT, -1)
        val key = prefs.getString(KEY_KEY, null) ?: return null
        if (port <= 0 || key.isBlank()) return null
        return AudexCredentials(host, port, key)
    }

    /** Forgets pairing and wipes anything synced from this device: Room rows + cached covers. */
    suspend fun unpair() = withContext(Dispatchers.IO) {
        prefs.edit { clear() }
        musicDao.clearAllAudexSongs()
        coverDir().deleteRecursively()
        _isPairedFlow.value = false
    }

    private fun request(creds: AudexCredentials, path: String): Request =
        Request.Builder()
            .url("${creds.base}$path")
            .header("Authorization", "Bearer ${creds.key}")
            .build()

    /** Verifies host/port/key against `/api/info`, persisting them on success. */
    suspend fun connect(host: String, port: Int, key: String): Result<AudexInfoResponse> =
        withContext(Dispatchers.IO) {
            val creds = AudexCredentials(host, port, key)
            try {
                http.newCall(request(creds, "/api/info")).execute().use { resp ->
                    if (!resp.isSuccessful) return@withContext Result.failure(Exception("HTTP ${resp.code}"))
                    val info = json.decodeFromString<AudexInfoResponse>(resp.body.string())
                    prefs.edit {
                        putString(KEY_HOST, host)
                        putInt(KEY_PORT, port)
                        putString(KEY_KEY, key)
                        putString(KEY_NAME, info.name.ifBlank { host })
                    }
                    _isPairedFlow.value = true
                    Result.success(info)
                }
            } catch (e: Exception) {
                Timber.e(e, "$TAG: connect to $host:$port failed")
                Result.failure(friendlyError(e))
            }
        }

    /**
     * Fetches the paired device's current library and projects it into the
     * unified `songs` table — in batches of [SYNC_BATCH_SIZE], each its own
     * `incrementalSyncMusicData` transaction, so [songsFlow]/the Library
     * screen show tracks appearing progressively instead of only once the
     * whole (possibly large, cover-art-heavy) sync finishes. [onProgress],
     * if given, is called after each batch commits with (done, total) — the
     * caller decides what to do with that (e.g. relay it as WorkManager
     * progress, as [com.theveloper.pixelplay.data.worker.AudexSyncWorker] does).
     */
    suspend fun syncLibrary(onProgress: ((done: Int, total: Int) -> Unit)? = null): Result<Int> =
        withContext(Dispatchers.IO) {
            val creds = readSavedCredentials() ?: return@withContext Result.failure(IllegalStateException("not paired"))
            try {
                val tracks = http.newCall(request(creds, "/api/library")).execute().use { resp ->
                    if (!resp.isSuccessful) return@withContext Result.failure(Exception("HTTP ${resp.code}"))
                    json.decodeFromString<AudexLibraryResponse>(resp.body.string()).tracks
                }

                val deviceLabel = pairedDeviceName ?: creds.host
                // Computed once over the *whole* list (cheap, no I/O) so a track's
                // album gets the correct total songCount regardless of which batch
                // happens to carry that AlbumEntity's insert. Keyed by albumArtist,
                // NOT the per-track artist — two tracks on the same album can have
                // different/differently-formatted track artists (e.g. a feature:
                // "Gay" vs "Gay, Lesbian"), and keying on that split one real album
                // into several. albumArtist is the tag that's supposed to be
                // consistent across an album's tracks; blank is fine too as long
                // as every track on the album leaves it blank the same way.
                val albumCounts = tracks.groupingBy { unifiedAlbumId(it.album.ifBlank { "Unknown Album" }, it.albumArtist) }
                    .eachCount()
                val existingIds = musicDao.getAllAudexSongIds()
                val currentIds = tracks.map { unifiedSongId(it.id) }.toSet()
                val deletedIds = existingIds.filter { it !in currentIds }

                var done = 0
                tracks.chunked(SYNC_BATCH_SIZE).forEachIndexed { batchIndex, batch ->
                    // One request per track's cover, run with bounded concurrency
                    // instead of sequentially — with a real library this was the
                    // actual source of "endless" pairing: each cover waited out
                    // its own timeout before the next one even started.
                    val coverUris: Map<String, String?> = coroutineScope {
                        val semaphore = Semaphore(COVER_FETCH_CONCURRENCY)
                        batch.filter { it.coverUrl != null }
                            .map { track ->
                                async {
                                    semaphore.withPermit { track.id to cacheCover(creds, track.id, track.coverUrl!!) }
                                }
                            }
                            .awaitAll()
                            .toMap()
                    }

                    val batchArtists = LinkedHashMap<Long, ArtistEntity>()
                    val batchAlbums = LinkedHashMap<Long, AlbumEntity>()
                    val batchCrossRefs = mutableListOf<SongArtistCrossRef>()

                    val batchSongs = batch.map { track ->
                        val songId = unifiedSongId(track.id)
                        val artistName = track.artist.ifBlank { "Unknown Artist" }
                        val artistId = unifiedArtistId(artistName)
                        batchArtists.putIfAbsent(
                            artistId,
                            ArtistEntity(id = artistId, name = artistName, trackCount = 0, imageUrl = null)
                        )
                        batchCrossRefs.add(SongArtistCrossRef(songId = songId, artistId = artistId, isPrimary = true))

                        val albumName = track.album.ifBlank { "Unknown Album" }
                        val albumId = unifiedAlbumId(albumName, track.albumArtist)
                        val coverUri = coverUris[track.id]
                        // Prefer the albumArtist tag for the album's own display
                        // artist/artistId; only fall back to this particular
                        // track's artist when albumArtist wasn't tagged at all.
                        val albumArtistName = track.albumArtist.ifBlank { artistName }
                        val albumArtistId = if (track.albumArtist.isBlank()) {
                            artistId
                        } else {
                            unifiedArtistId(albumArtistName).also { id ->
                                batchArtists.putIfAbsent(id, ArtistEntity(id = id, name = albumArtistName, trackCount = 0, imageUrl = null))
                            }
                        }
                        batchAlbums.putIfAbsent(
                            albumId,
                            AlbumEntity(
                                id = albumId,
                                title = albumName,
                                artistName = albumArtistName,
                                artistId = albumArtistId,
                                songCount = albumCounts[albumId] ?: 0,
                                dateAdded = System.currentTimeMillis(),
                                year = 0,
                                albumArtUriString = coverUri,
                                albumArtist = track.albumArtist.ifBlank { null }
                            )
                        )

                        SongEntity(
                            id = songId,
                            title = track.title.ifBlank { "Unknown" },
                            artistName = artistName,
                            artistId = artistId,
                            albumArtist = track.albumArtist.ifBlank { null },
                            albumName = albumName,
                            albumId = albumId,
                            contentUriString = track.url,
                            albumArtUriString = coverUri,
                            duration = (track.duration * 1000).toLong(),
                            genre = null,
                            filePath = "",
                            parentDirectoryPath = "/Audex/$deviceLabel",
                            dateAdded = System.currentTimeMillis(),
                            mimeType = null,
                            bitrate = null,
                            sampleRate = null,
                            sourceType = SourceType.AUDEX
                        )
                    }

                    musicDao.incrementalSyncMusicData(
                        songs = batchSongs,
                        albums = batchAlbums.values.toList(),
                        artists = batchArtists.values.toList(),
                        crossRefs = batchCrossRefs,
                        // Only the first batch carries the deletions — no need to repeat them.
                        deletedSongIds = if (batchIndex == 0) deletedIds else emptyList()
                    )

                    done += batch.size
                    onProgress?.invoke(done, tracks.size)
                }

                Result.success(tracks.size)
            } catch (e: Exception) {
                Timber.e(e, "$TAG: syncLibrary failed")
                Result.failure(friendlyError(e))
            }
        }

    /** Turns the usual raw network exceptions into messages that tell the user what to actually check. */
    private fun friendlyError(e: Exception): Exception = when (e) {
        is SocketTimeoutException -> Exception("Timed out reaching that device — check it's on and on the same network", e)
        is UnknownHostException -> Exception("Couldn't resolve \"${e.message}\" — check the host/IP", e)
        is ConnectException -> Exception("Couldn't reach that address — check the host, port, and that LAN sharing is on", e)
        else -> e
    }

    private fun unifiedSongId(trackId: String) =
        -(AUDEX_SONG_ID_OFFSET + trackId.hashCode().toLong().absoluteValue)

    // Keyed by albumArtist (blank counts as its own consistent key), NOT the
    // per-track artist — see the comment at the syncLibrary() call site.
    private fun unifiedAlbumId(albumName: String, albumArtist: String) =
        -(AUDEX_ALBUM_ID_OFFSET + "$albumArtist|$albumName".lowercase().hashCode().toLong().absoluteValue)

    private fun unifiedArtistId(artistName: String) =
        -(AUDEX_ARTIST_ID_OFFSET + artistName.lowercase().hashCode().toLong().absoluteValue)

    private fun coverDir(): File = File(context.filesDir, COVER_DIR_NAME).apply { mkdirs() }

    /** Downloads a track's cover once and returns a content:// URI for the cached file, or null on failure. */
    private fun cacheCover(creds: AudexCredentials, trackId: String, coverUrl: String): String? {
        val file = File(coverDir(), "$trackId.jpg")
        if (!file.exists()) {
            try {
                val req = Request.Builder()
                    .url(coverUrl)
                    .header("Authorization", "Bearer ${creds.key}")
                    .build()
                http.newCall(req).execute().use { resp ->
                    if (!resp.isSuccessful) return null
                    file.outputStream().use { out -> resp.body.byteStream().copyTo(out) }
                }
            } catch (e: Exception) {
                Timber.w(e, "$TAG: cover download failed for $trackId")
                return null
            }
        }
        return try {
            FileProvider.getUriForFile(context, "${context.packageName}.provider", file).toString()
        } catch (e: Exception) {
            null
        }
    }
}
