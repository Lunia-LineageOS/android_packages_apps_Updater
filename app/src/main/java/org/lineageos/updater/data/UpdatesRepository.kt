/*
 * SPDX-FileCopyrightText: The LineageOS Project
 * SPDX-License-Identifier: Apache-2.0
 */

package org.lineageos.updater.data

import android.content.Context
import android.os.Build
import android.os.UpdateEngine
import android.ota.nano.OtaPackageMetadata.OtaMetadata
import android.util.Log
import androidx.preference.PreferenceManager
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.withContext
import kotlinx.serialization.SerializationException
import org.lineageos.updater.data.source.local.UpdatesLocalDataSource
import org.lineageos.updater.data.source.network.NetworkUpdate
import org.lineageos.updater.data.source.network.UpdatesNetworkDataSource
import org.lineageos.updater.data.source.network.parsePackageFileRanges
import org.lineageos.updater.data.source.network.toIncrementalUpdate
import org.lineageos.updater.data.source.network.toUpdate
import org.lineageos.updater.deviceinfo.DeviceInfoUtils
import org.lineageos.updater.download.SingleRangeHttpFetcher
import org.lineageos.updater.misc.Constants
import org.lineageos.updater.misc.Utils
import org.lineageos.updater.notifications.NotificationHelper
import org.lineageos.updater.util.NetworkMonitor
import org.lineageos.updater.util.SystemUpdateInfoPublisher
import org.json.JSONObject
import java.io.File
import java.io.IOException

private const val TAG = "UpdatesRepository"

class UpdatesRepository(
    private val context: Context,
    private val networkMonitor: NetworkMonitor,
    private val notificationHelper: NotificationHelper,
    private val networkDataSource: UpdatesNetworkDataSource,
    private val localDataSource: UpdatesLocalDataSource,
    private val systemUpdateInfoPublisher: SystemUpdateInfoPublisher,
) {
    fun observeLocalUpdates(): Flow<List<Update>> = localDataSource.observeUpdates()

    /**
     * Fetches available updates from the server, syncs the local database, and posts a
     * notification if new updates are found. Callers observe [observeLocalUpdates] for results —
     * Room only emits when the stored data actually changes.
     *
     * @return the timestamp of the fetch, or null if skipped due to no network.
     * @throws IOException on network or HTTP errors.
     * @throws SerializationException if the response cannot be parsed.
     */
    suspend fun fetchUpdates(): Long? {
        if (!networkMonitor.currentNetworkState.isOnline) return null

        val localUpdates = withContext(Dispatchers.IO) {
            localDataSource.getUpdates()
        }.associateBy { it.downloadId }

        withContext(Dispatchers.IO) {
            localUpdates.values.filter {
                it.downloadId != Update.LOCAL_ID && !filterUpdates(it)
            }.forEach {
                it.file?.delete()
                localDataSource.removeUpdate(it.downloadId)
            }
        }

        val networkUpdates = withContext(Dispatchers.IO) {
            val network = networkDataSource.fetchUpdates()
            val applicable = network.filter { isIncrementalApplicable(it) }
            persistIncrementalLinks(applicable)
            network.flatMap { update ->
                val incremental = update.toIncrementalUpdate().takeIf { update in applicable }
                listOfNotNull(update.toUpdate(), incremental)
            }.filter { filterUpdates(it) }
        }

        if (networkUpdates.isEmpty()) {
            systemUpdateInfoPublisher.publish()
            return System.currentTimeMillis()
        }

        val networkIds = networkUpdates.map { it.downloadId }.toSet()

        if (localUpdates.isNotEmpty() && networkUpdates.any { it.downloadId !in localUpdates }) {
            notificationHelper.showNewUpdatesNotification()
        }

        withContext(Dispatchers.IO) {
            // Merge local state into each network update and upsert into the DB.
            // Room's observeUpdates() Flow will emit automatically if anything changed.
            networkUpdates.forEach { networkUpdate ->
                val local = localUpdates[networkUpdate.downloadId]
                val update = if (local != null && local.status.persistentStatus > 0) {
                    networkUpdate.copy(status = local.status, file = local.file)
                } else {
                    networkUpdate
                }
                localDataSource.addUpdate(update)
            }

            // Delete temp files and DB entries for updates no longer advertised by the server.
            localUpdates.values.filter {
                it.downloadId !in networkIds && it.downloadId != Update.LOCAL_ID &&
                        it.downloadUrl != null
            }.forEach {
                it.file?.delete()
                localDataSource.removeUpdate(it.downloadId)
            }
        }
        systemUpdateInfoPublisher.publish()

        return System.currentTimeMillis()
    }

    private fun persistIncrementalLinks(network: List<NetworkUpdate>) {
        val links = JSONObject()
        network.forEach { update ->
            update.incremental?.firstOrNull()?.let { delta ->
                links.put(update.files[0].sha256, delta.url)
            }
        }
        PreferenceManager.getDefaultSharedPreferences(context).edit()
            .putString(Constants.PREF_INCREMENTAL_LINKS, links.toString()).apply()
    }

    /**
     * Checks that the incremental applies to the running build: update_engine verifies A/B
     * payloads, and non-A/B packages must list this build as their source.
     */
    private fun isIncrementalApplicable(update: NetworkUpdate): Boolean {
        val delta = update.incremental?.firstOrNull() ?: return false
        val ranges = delta.otaPropertyFiles?.parsePackageFileRanges() ?: return false
        val fetcher = SingleRangeHttpFetcher(delta.url)
        return try {
            if (DeviceInfoUtils.isABDevice) {
                val range = ranges[Constants.AB_PAYLOAD_METADATA_PATH] ?: return false
                val metadata = File(Utils.getDownloadPath(context), "${delta.sha256}.metadata")
                try {
                    metadata.writeBytes(fetcher.download(range.offset, range.size))
                    metadata.setReadable(true, false)
                    UpdateEngine().verifyPayloadMetadata(metadata.path)
                } finally {
                    metadata.delete()
                }
            } else {
                val range = ranges[Constants.OTA_METADATA_PB_PATH] ?: return false
                val metadata = OtaMetadata.parseFrom(fetcher.download(range.offset, range.size))
                Build.FINGERPRINT in metadata.precondition.build
            }
        } catch (e: Exception) {
            Log.w(TAG, "${delta.filename} can't be applied to this build", e)
            false
        }
    }

    private fun filterUpdates(update: Update): Boolean {
        val isCurrentBuild = update.timestamp == DeviceInfoUtils.buildDateTimestamp
        val isOlderBuild = update.timestamp < DeviceInfoUtils.buildDateTimestamp

        if (!DeviceInfoUtils.isDowngradingAllowed && (isOlderBuild || isCurrentBuild)) {
            Log.d(TAG, "${update.name} is not newer than the current build")
            return false
        }

        if (update.osSdkLevel < DeviceInfoUtils.sdkLevel) {
            Log.d(TAG, "${update.name} is older than current Android version")
            return false
        }

        if (!update.type.equals(DeviceInfoUtils.releaseType, ignoreCase = true)) {
            Log.d(TAG, "${update.name} has type ${update.type}")
            return false
        }
        return true
    }
}
