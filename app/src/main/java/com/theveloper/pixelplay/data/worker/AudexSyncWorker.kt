package com.theveloper.pixelplay.data.worker

import android.content.Context
import androidx.hilt.work.HiltWorker
import androidx.work.CoroutineWorker
import androidx.work.OneTimeWorkRequestBuilder
import androidx.work.WorkerParameters
import androidx.work.workDataOf
import com.theveloper.pixelplay.data.audex.AudexRepository
import dagger.assisted.Assisted
import dagger.assisted.AssistedInject
import timber.log.Timber

/**
 * Runs [AudexRepository.syncLibrary] as background work so it survives
 * leaving [com.theveloper.pixelplay.presentation.audex.AudexActivity] (or the
 * app) — mirrors [NavidromeSyncWorker]'s shape. Progress is relayed via
 * [setProgressAsync] so [com.theveloper.pixelplay.presentation.audex.AudexViewModel]
 * can show it; the actual song list is read reactively from Room
 * (`AudexRepository.songsFlow`), not from this worker's result, since
 * `syncLibrary` commits in batches as it goes.
 */
@HiltWorker
class AudexSyncWorker @AssistedInject constructor(
    @Assisted appContext: Context,
    @Assisted workerParams: WorkerParameters,
    private val repository: AudexRepository
) : CoroutineWorker(appContext, workerParams) {

    override suspend fun doWork(): Result {
        return repository.syncLibrary { done, total ->
            setProgressAsync(workDataOf(PROGRESS_DONE to done, PROGRESS_TOTAL to total))
        }.fold(
            onSuccess = { Result.success() },
            onFailure = { e ->
                Timber.e(e, "AudexSyncWorker: sync failed")
                Result.failure(workDataOf(ERROR_MESSAGE to e.message))
            }
        )
    }

    companion object {
        const val WORK_NAME = "audex_sync"
        const val PROGRESS_DONE = "done"
        const val PROGRESS_TOTAL = "total"
        const val ERROR_MESSAGE = "error_message"

        fun request() = OneTimeWorkRequestBuilder<AudexSyncWorker>().build()
    }
}
