package com.theveloper.pixelplay.presentation.audex

import androidx.lifecycle.ViewModel
import androidx.lifecycle.viewModelScope
import androidx.work.ExistingWorkPolicy
import androidx.work.WorkInfo
import androidx.work.WorkManager
import com.theveloper.pixelplay.data.audex.AudexRepository
import com.theveloper.pixelplay.data.model.Song
import com.theveloper.pixelplay.data.worker.AudexSyncWorker
import dagger.hilt.android.lifecycle.HiltViewModel
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.SharingStarted
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.combine
import kotlinx.coroutines.flow.stateIn
import kotlinx.coroutines.launch
import javax.inject.Inject

/** Pairing itself is a single fast HTTP call — kept separate from the (possibly slow) background library sync. */
sealed interface PairingPhase {
    data object Disconnected : PairingPhase
    data object Pairing : PairingPhase
    data class Error(val message: String) : PairingPhase
    data object Paired : PairingPhase
}

sealed interface SyncStatus {
    data object Idle : SyncStatus
    data class Running(val done: Int, val total: Int) : SyncStatus
    data class Failed(val message: String) : SyncStatus
}

sealed interface AudexUiState {
    data object Disconnected : AudexUiState
    data object Pairing : AudexUiState
    data class Error(val message: String) : AudexUiState
    data class Connected(val deviceName: String, val songs: List<Song>, val sync: SyncStatus) : AudexUiState
}

@HiltViewModel
class AudexViewModel @Inject constructor(
    private val repository: AudexRepository,
    private val workManager: WorkManager
) : ViewModel() {

    private val pairingState = MutableStateFlow<PairingPhase>(
        if (repository.isPaired) PairingPhase.Paired else PairingPhase.Disconnected
    )

    val state: StateFlow<AudexUiState> = combine(
        pairingState,
        repository.songsFlow,
        workManager.getWorkInfosForUniqueWorkFlow(AudexSyncWorker.WORK_NAME)
    ) { pairing, songs, workInfos ->
        when (pairing) {
            is PairingPhase.Disconnected -> AudexUiState.Disconnected
            is PairingPhase.Pairing -> AudexUiState.Pairing
            is PairingPhase.Error -> AudexUiState.Error(pairing.message)
            is PairingPhase.Paired -> AudexUiState.Connected(
                deviceName = repository.pairedDeviceName ?: "Audex device",
                songs = songs,
                sync = workInfos.firstOrNull()?.toSyncStatus() ?: SyncStatus.Idle
            )
        }
    }.stateIn(viewModelScope, SharingStarted.WhileSubscribed(5_000), AudexUiState.Disconnected)

    init {
        // Cheap no-op if nothing's pending, so this is safe to call every time the screen opens.
        if (repository.isPaired) enqueueSync(ExistingWorkPolicy.KEEP)
    }

    private fun WorkInfo.toSyncStatus(): SyncStatus = when (state) {
        WorkInfo.State.RUNNING, WorkInfo.State.ENQUEUED -> SyncStatus.Running(
            done = progress.getInt(AudexSyncWorker.PROGRESS_DONE, 0),
            total = progress.getInt(AudexSyncWorker.PROGRESS_TOTAL, 0)
        )
        WorkInfo.State.FAILED -> SyncStatus.Failed(
            outputData.getString(AudexSyncWorker.ERROR_MESSAGE) ?: "Sync failed"
        )
        else -> SyncStatus.Idle
    }

    fun connect(host: String, portText: String, key: String) {
        val port = portText.toIntOrNull()
        if (host.isBlank() || key.isBlank() || port == null) {
            pairingState.value = PairingPhase.Error("Enter a valid host, port and key")
            return
        }
        pairingState.value = PairingPhase.Pairing
        viewModelScope.launch {
            repository.connect(host, port, key)
                .onSuccess {
                    pairingState.value = PairingPhase.Paired
                    enqueueSync(ExistingWorkPolicy.REPLACE)
                }
                .onFailure { pairingState.value = PairingPhase.Error(it.message ?: "Connection failed") }
        }
    }

    /** Manual "Refresh" — forces a new sync even if one just ran. */
    fun sync() = enqueueSync(ExistingWorkPolicy.REPLACE)

    fun cancelSync() = workManager.cancelUniqueWork(AudexSyncWorker.WORK_NAME)

    fun disconnect() {
        workManager.cancelUniqueWork(AudexSyncWorker.WORK_NAME)
        viewModelScope.launch {
            repository.unpair()
            pairingState.value = PairingPhase.Disconnected
        }
    }

    private fun enqueueSync(policy: ExistingWorkPolicy) {
        workManager.enqueueUniqueWork(AudexSyncWorker.WORK_NAME, policy, AudexSyncWorker.request())
    }
}
