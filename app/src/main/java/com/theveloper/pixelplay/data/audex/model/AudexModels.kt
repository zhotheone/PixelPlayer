package com.theveloper.pixelplay.data.audex.model

import kotlinx.serialization.Serializable

/** A paired Audex desktop instance sharing its library over the LAN. */
data class AudexCredentials(val host: String, val port: Int, val key: String) {
    val base: String get() = "http://$host:$port"
}

/**
 * One track as returned by Audex's `/api/library` (see lan.js `publicTrack()`
 * in the audex-player repo). `url`/`coverUrl` are already HMAC-signed by the
 * server, so they're streamable as-is — no per-request auth needed.
 */
@Serializable
data class AudexTrack(
    val id: String,
    val title: String = "",
    val artist: String = "",
    val album: String = "",
    val albumArtist: String = "",
    val trackNo: String = "",
    val discNo: String = "",
    val duration: Double = 0.0, // seconds
    val url: String,
    val coverUrl: String? = null,
)

@Serializable
data class AudexLibraryResponse(val tracks: List<AudexTrack> = emptyList())

@Serializable
data class AudexInfoResponse(val deviceId: String = "", val name: String = "")
