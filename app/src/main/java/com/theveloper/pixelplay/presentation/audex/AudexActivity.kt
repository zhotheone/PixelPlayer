package com.theveloper.pixelplay.presentation.audex

import android.os.Bundle
import androidx.activity.ComponentActivity
import androidx.activity.compose.setContent
import androidx.activity.enableEdgeToEdge
import androidx.activity.viewModels
import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.automirrored.rounded.ArrowBack
import androidx.compose.material.icons.rounded.PlayArrow
import androidx.compose.material.icons.rounded.LinkOff
import androidx.compose.material.icons.rounded.Refresh
import androidx.compose.material.icons.rounded.Close
import androidx.compose.material3.Button
import androidx.compose.material3.CircularProgressIndicator
import androidx.compose.material3.ExperimentalMaterial3Api
import androidx.compose.material3.Icon
import androidx.compose.material3.IconButton
import androidx.compose.material3.LinearProgressIndicator
import androidx.compose.material3.ListItem
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.OutlinedTextField
import androidx.compose.material3.Scaffold
import androidx.compose.material3.Surface
import androidx.compose.material3.Text
import androidx.compose.material3.TopAppBar
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.size
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.unit.dp
import androidx.hilt.lifecycle.viewmodel.compose.hiltViewModel
import androidx.lifecycle.compose.collectAsStateWithLifecycle
import androidx.media3.common.util.UnstableApi
import com.theveloper.pixelplay.data.model.Song
import com.theveloper.pixelplay.presentation.viewmodel.PlayerViewModel
import com.theveloper.pixelplay.ui.theme.PixelPlayTheme
import dagger.hilt.android.AndroidEntryPoint

/**
 * Standalone screen for pairing with an Audex desktop instance sharing its
 * library over the LAN. Pairing/syncing is handled by
 * [com.theveloper.pixelplay.data.audex.AudexRepository.syncLibrary], which
 * also projects the tracks into the unified `songs` table (see
 * [com.theveloper.pixelplay.data.database.SourceType.AUDEX]) so they show up
 * in Library/Albums/Artists/Search like any other source. This screen is
 * just a quick-browse/manage-pairing view on top of that; tapping a track
 * plays it directly via [PlayerViewModel.playSongs] (same pattern
 * [com.theveloper.pixelplay.ExternalPlayerActivity] uses to drive the shared
 * playback service from outside the main nav graph).
 */
@UnstableApi
@AndroidEntryPoint
class AudexActivity : ComponentActivity() {

    private val playerViewModel: PlayerViewModel by viewModels()

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        enableEdgeToEdge()
        setContent {
            PixelPlayTheme {
                val viewModel: AudexViewModel = hiltViewModel()
                AudexScreen(
                    viewModel = viewModel,
                    onPlay = { songs, song -> playerViewModel.playSongs(songs, song, queueName = "Audex") },
                    onClose = { finish() }
                )
            }
        }
    }
}

@OptIn(ExperimentalMaterial3Api::class)
@Composable
private fun AudexScreen(
    viewModel: AudexViewModel,
    onPlay: (List<Song>, Song) -> Unit,
    onClose: () -> Unit
) {
    val state by viewModel.state.collectAsStateWithLifecycle()

    Scaffold(
        topBar = {
            TopAppBar(
                title = { Text("Audex") },
                navigationIcon = {
                    IconButton(onClick = onClose) {
                        Icon(Icons.AutoMirrored.Rounded.ArrowBack, contentDescription = "Back")
                    }
                },
                actions = {
                    if (state is AudexUiState.Connected) {
                        IconButton(onClick = viewModel::sync) {
                            Icon(Icons.Rounded.Refresh, contentDescription = "Sync library")
                        }
                        IconButton(onClick = viewModel::disconnect) {
                            Icon(Icons.Rounded.LinkOff, contentDescription = "Disconnect")
                        }
                    }
                }
            )
        }
    ) { padding ->
        Box(
            modifier = Modifier
                .fillMaxSize()
                .padding(padding)
        ) {
            when (val s = state) {
                is AudexUiState.Disconnected -> ConnectForm(onConnect = viewModel::connect)

                is AudexUiState.Pairing -> CircularProgressIndicator(Modifier.align(Alignment.Center))

                is AudexUiState.Error -> Column(
                    modifier = Modifier
                        .fillMaxWidth()
                        .align(Alignment.Center)
                        .padding(24.dp)
                ) {
                    Text(s.message, color = MaterialTheme.colorScheme.error)
                    Spacer(Modifier.height(16.dp))
                    ConnectForm(onConnect = viewModel::connect)
                }

                is AudexUiState.Connected -> Column(Modifier.fillMaxSize()) {
                    SyncBanner(sync = s.sync, deviceName = s.deviceName, onCancel = viewModel::cancelSync)
                    if (s.songs.isEmpty() && s.sync !is SyncStatus.Running) {
                        Text(
                            text = "No tracks synced yet from ${s.deviceName}",
                            style = MaterialTheme.typography.bodyMedium,
                            color = MaterialTheme.colorScheme.onSurfaceVariant,
                            modifier = Modifier.padding(16.dp)
                        )
                    } else {
                        Text(
                            text = "${s.songs.size} tracks synced from ${s.deviceName} — also in your Library",
                            style = MaterialTheme.typography.labelLarge,
                            color = MaterialTheme.colorScheme.onSurfaceVariant,
                            modifier = Modifier.padding(16.dp)
                        )
                    }
                    LazyColumn {
                        items(s.songs, key = { it.id }) { song ->
                            ListItem(
                                headlineContent = { Text(song.title, maxLines = 1, overflow = TextOverflow.Ellipsis) },
                                supportingContent = { Text(song.artist, maxLines = 1, overflow = TextOverflow.Ellipsis) },
                                trailingContent = { Icon(Icons.Rounded.PlayArrow, contentDescription = "Play") },
                                modifier = Modifier.clickable { onPlay(s.songs, song) }
                            )
                        }
                    }
                }
            }
        }
    }
}

@Composable
private fun SyncBanner(sync: SyncStatus, deviceName: String, onCancel: () -> Unit) {
    when (sync) {
        is SyncStatus.Running -> Surface(color = MaterialTheme.colorScheme.surfaceContainerHigh) {
            Column(Modifier.padding(16.dp)) {
                Row(verticalAlignment = Alignment.CenterVertically) {
                    Text(
                        text = if (sync.total > 0) {
                            "Syncing from $deviceName — ${sync.done}/${sync.total}"
                        } else {
                            "Syncing from $deviceName…"
                        },
                        style = MaterialTheme.typography.labelLarge,
                        modifier = Modifier.weight(1f)
                    )
                    IconButton(onClick = onCancel, modifier = Modifier.size(32.dp)) {
                        Icon(Icons.Rounded.Close, contentDescription = "Cancel sync")
                    }
                }
                Spacer(Modifier.height(8.dp))
                if (sync.total > 0) {
                    LinearProgressIndicator(
                        progress = { sync.done.toFloat() / sync.total },
                        modifier = Modifier.fillMaxWidth()
                    )
                } else {
                    LinearProgressIndicator(modifier = Modifier.fillMaxWidth())
                }
            }
        }
        is SyncStatus.Failed -> Surface(color = MaterialTheme.colorScheme.errorContainer) {
            Text(
                text = "Last sync failed: ${sync.message}",
                color = MaterialTheme.colorScheme.onErrorContainer,
                style = MaterialTheme.typography.bodySmall,
                modifier = Modifier.padding(16.dp)
            )
        }
        is SyncStatus.Idle -> Unit
    }
}

@Composable
private fun ConnectForm(onConnect: (host: String, port: String, key: String) -> Unit) {
    var host by remember { mutableStateOf("") }
    var port by remember { mutableStateOf("8422") }
    var key by remember { mutableStateOf("") }

    Column(modifier = Modifier.fillMaxWidth().padding(24.dp)) {
        Text("Connect to an Audex device on your LAN", style = MaterialTheme.typography.titleMedium)
        Text(
            "Enable LAN sharing in Audex's settings on the desktop app, then enter its address and network key here.",
            style = MaterialTheme.typography.bodySmall,
            color = MaterialTheme.colorScheme.onSurfaceVariant,
            modifier = Modifier.padding(top = 4.dp, bottom = 16.dp)
        )
        OutlinedTextField(
            value = host,
            onValueChange = { host = it },
            label = { Text("Host / IP") },
            singleLine = true,
            modifier = Modifier.fillMaxWidth()
        )
        Spacer(Modifier.height(8.dp))
        OutlinedTextField(
            value = port,
            onValueChange = { port = it },
            label = { Text("Port") },
            singleLine = true,
            modifier = Modifier.fillMaxWidth()
        )
        Spacer(Modifier.height(8.dp))
        OutlinedTextField(
            value = key,
            onValueChange = { key = it },
            label = { Text("Network key") },
            singleLine = true,
            modifier = Modifier.fillMaxWidth()
        )
        Spacer(Modifier.height(16.dp))
        Button(onClick = { onConnect(host.trim(), port.trim(), key) }, modifier = Modifier.fillMaxWidth()) {
            Text("Connect")
        }
    }
}
