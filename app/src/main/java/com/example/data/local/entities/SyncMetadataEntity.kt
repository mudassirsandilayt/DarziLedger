package com.example.data.local.entities

import androidx.room.Entity
import androidx.room.PrimaryKey
import com.example.core.model.SyncState

@Entity(tableName = "sync_metadata")
data class SyncMetadataEntity(
  @PrimaryKey val deviceId: String,
  val lastSuccessfulSyncAt: Long? = null,
  val lastSyncAttemptAt: Long? = null,
  val syncStatus: SyncState = SyncState.IDLE,
  val lastError: String? = null,
  val customersLastPulledAt: Long = 0L,
  val measurementsLastPulledAt: Long = 0L,
  val ordersLastPulledAt: Long = 0L,
  val paymentsLastPulledAt: Long = 0L,
  val reservationsLastPulledAt: Long = 0L,
)
