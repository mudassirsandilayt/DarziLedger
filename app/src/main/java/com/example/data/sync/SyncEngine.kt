package com.example.data.sync

import android.util.Log
import androidx.room.withTransaction
import com.example.core.model.DeviceRegistrationStatus
import com.example.core.model.ShopUserRole
import com.example.core.model.SyncEntityType
import com.example.core.model.SyncOperation
import com.example.core.model.SyncState
import com.example.core.model.SyncStatus
import com.example.data.local.database.DarziDatabase
import com.example.data.local.entities.CustomerEntity
import com.example.data.local.entities.MeasurementEntity
import com.example.data.local.entities.OrderEntity
import com.example.data.local.entities.OrderNumberReservationEntity
import com.example.data.local.entities.OrderSyncLogEntity
import com.example.data.local.entities.OrderSyncLogOperation
import com.example.data.local.entities.OrderSyncLogStatus
import com.example.data.local.entities.PaymentEntity
import com.example.data.local.entities.ShopEntity
import com.example.data.local.entities.SyncMetadataEntity
import com.example.data.local.entities.SyncOutboxEntity
import com.example.data.local.repository.DeviceSyncRepository
import com.example.data.local.repository.OrderRepository
import com.example.data.local.repository.ShopRepository
import com.example.data.remote.AuthSessionProvider
import com.example.data.remote.CustomerDoc
import com.example.data.remote.MeasurementDoc
import com.google.firebase.firestore.DocumentChange
import com.example.data.remote.OperationType
import com.example.data.remote.OrderDoc
import com.example.data.remote.OrderReservationDoc
import com.example.data.remote.PaymentDoc
import com.example.data.remote.handleFirestoreError
import com.example.data.sync.SyncMappers.toLocalEntity
import com.google.firebase.firestore.DocumentSnapshot
import com.google.firebase.firestore.FieldValue
import com.google.firebase.firestore.FirebaseFirestore
import com.google.firebase.Timestamp
import com.google.firebase.firestore.FirebaseFirestoreException
import com.google.firebase.firestore.snapshots
import java.util.Date
import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.flow.collectLatest
import kotlinx.coroutines.flow.combine
import kotlinx.coroutines.flow.distinctUntilChanged
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.flow.launchIn
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.tasks.await
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json

data class SyncSummary(
  val pushedCount: Int = 0,
  val pulledCount: Int = 0,
  val assignedOrderNumbers: Int = 0,
  val conflictsDetected: Int = 0,
)

class SyncEngine(
  private val database: DarziDatabase,
  private val db: FirebaseFirestore,
  private val authSessionProvider: AuthSessionProvider,
  private val connectivityMonitor: ConnectivityMonitor,
  private val deviceSyncRepository: DeviceSyncRepository,
  private val shopRepository: ShopRepository,
  private val orderRepository: OrderRepository,
) {
  private val customerDao = database.customerDao()
  private val measurementDao = database.measurementDao()
  private val orderDao = database.orderDao()
  private val deviceSyncDao = database.deviceSyncDao()
  private val outboxDao = database.syncOutboxDao()
  private val orderSyncLogDao = database.orderSyncLogDao()
  private val remoteApplier = SyncRemoteApplier(database)

  private val json = Json {
    ignoreUnknownKeys = true
    encodeDefaults = true
  }

  private val pushMutex = Mutex()
  private val pullMutex = Mutex()
  private val pushResyncRequested = AtomicBoolean(false)
  private val isPushActive = AtomicBoolean(false)
  private val isPullActive = AtomicBoolean(false)
  private var activeSyncJob: Job? = null
  private var activeShopId: String? = null

  private val _syncState = MutableStateFlow(SyncState.IDLE)
  val syncState: StateFlow<SyncState> = _syncState.asStateFlow()

  private val _lastError = MutableStateFlow<String?>(null)
  val lastError: StateFlow<String?> = _lastError.asStateFlow()

  fun startRealtimeSync(shopId: String, scope: CoroutineScope) {
    if (shopId.isBlank()) return
    if (activeShopId == shopId && activeSyncJob?.isActive == true) return
    stopRealtimeSync()
    activeShopId = shopId

    val job = SupervisorJob(scope.coroutineContext[Job])
    val ioScope = CoroutineScope(scope.coroutineContext + job + Dispatchers.IO)
    activeSyncJob = job

    val listenersStarted = AtomicBoolean(false)

    // Run pending outbox push & reservation check cleanly BEFORE opening the 5 full-collection
    // snapshot streams so initial Firestore queries/transactions are never starved behind bulk downloads.
    combine(
      connectivityMonitor.isOnline,
      deviceSyncRepository.observeDeviceConfig(),
    ) { online, device ->
      online to (device?.registrationStatus == DeviceRegistrationStatus.AUTHORIZED)
    }
      .distinctUntilChanged()
      .onEach { (online, authorized) ->
        if (!online) {
          _syncState.value = SyncState.OFFLINE
          if (authorized && listenersStarted.compareAndSet(false, true)) {
            attachSnapshotListeners(shopId, ioScope)
          }
        } else if (authorized) {
          ioScope.launch {
            pushPendingOutboxNow(forceRetry = false)
            if (listenersStarted.compareAndSet(false, true)) {
              attachSnapshotListeners(shopId, ioScope)
            }
          }
        }
      }
      .launchIn(ioScope)

    // Fast push-only auto-flush whenever local PENDING outbox items arrive (no blocking full pull)
    combine(
      outboxDao.observePendingCount(),
      outboxDao.observeLatestPendingCreatedAt(),
    ) { pending, latestPendingAt ->
      (pending > 0) to latestPendingAt
    }
      .distinctUntilChanged()
      .onEach { (hasPending, latestPendingAt) ->
        if (hasPending && latestPendingAt != null && connectivityMonitor.isOnline.value) {
          val device = deviceSyncDao.getDeviceConfig()
          if (device?.registrationStatus == DeviceRegistrationStatus.AUTHORIZED) {
            triggerPushOrQueue(ioScope)
          }
        }
      }
      .launchIn(ioScope)

    // Automatic retry wakeup timer when FAILED outbox events reach nextRetryAt
    ioScope.launch {
      combine(
        outboxDao.observeNextFailedRetryAt(),
        connectivityMonitor.isOnline,
      ) { nextRetryAt, online ->
        if (online) nextRetryAt else null
      }
        .distinctUntilChanged()
        .collectLatest { nextRetryAt ->
          if (nextRetryAt == null) return@collectLatest
          val delayMs = (nextRetryAt - System.currentTimeMillis()).coerceAtLeast(100L)
          delay(delayMs)
          val device = deviceSyncDao.getDeviceConfig()
          if (connectivityMonitor.isOnline.value &&
            device?.registrationStatus == DeviceRegistrationStatus.AUTHORIZED
          ) {
            pushPendingOutboxNow(forceRetry = false)
          }
        }
    }
  }

  private fun attachSnapshotListeners(shopId: String, ioScope: CoroutineScope) {
    // Real-time delta snapshot listeners on Dispatchers.IO (apply changes directly without re-enqueuing outbox)
    observeRemoteCollection(shopId, "customers", ioScope) { docs ->
      val localShopId = shopRepository.ensureDefaultShop().shopId
      val entities = docs.mapNotNull { doc ->
        doc.toObject(CustomerDoc::class.java, DocumentSnapshot.ServerTimestampBehavior.ESTIMATE)
          ?.toLocalEntity(localShopId)
      }
      remoteApplier.applyRemoteCustomers(entities)
    }

    observeRemoteCollection(shopId, "measurements", ioScope) { docs ->
      val localShopId = shopRepository.ensureDefaultShop().shopId
      val entities = docs.mapNotNull { doc ->
        doc.toObject(MeasurementDoc::class.java, DocumentSnapshot.ServerTimestampBehavior.ESTIMATE)
          ?.toLocalEntity()
      }
      remoteApplier.applyRemoteMeasurements(
        remoteList = entities,
        fetchMissingCustomers = { missingIds -> fetchMissingCustomers(shopId, localShopId, missingIds) },
      )
    }

    observeRemoteCollection(shopId, "orders", ioScope) { docs ->
      val localShopId = shopRepository.ensureDefaultShop().shopId
      val entities = docs.mapNotNull { doc ->
        doc.toObject(OrderDoc::class.java, DocumentSnapshot.ServerTimestampBehavior.ESTIMATE)
          ?.toLocalEntity(localShopId)
      }
      remoteApplier.applyRemoteOrders(
        remoteList = entities,
        fetchMissingCustomers = { missingIds -> fetchMissingCustomers(shopId, localShopId, missingIds) },
      )
      markRealtimeSnapshotSynced()
    }

    observeRemoteCollection(shopId, "payments", ioScope) { docs ->
      val localShopId = shopRepository.ensureDefaultShop().shopId
      val entities = docs.mapNotNull { doc ->
        doc.toObject(PaymentDoc::class.java, DocumentSnapshot.ServerTimestampBehavior.ESTIMATE)
          ?.toLocalEntity()
      }
      remoteApplier.applyRemotePayments(
        remoteList = entities,
        fetchMissingOrders = { missingOrderIds -> fetchMissingOrders(shopId, localShopId, missingOrderIds) },
        fetchMissingCustomers = { missingCustIds -> fetchMissingCustomers(shopId, localShopId, missingCustIds) },
      )
    }

    observeRemoteCollection(shopId, "order_reservations", ioScope) { docs ->
      val localDeviceId = deviceSyncDao.getDeviceConfig()?.deviceId ?: return@observeRemoteCollection
      val entities = docs.mapNotNull { doc ->
        doc.toObject(OrderReservationDoc::class.java, DocumentSnapshot.ServerTimestampBehavior.ESTIMATE)
      }.filter { it.deviceId == localDeviceId }.map { it.toLocalEntity() }
      remoteApplier.applyRemoteReservations(entities)
    }
  }

  private suspend fun markRealtimeSnapshotSynced() {
    val currentUid = authSessionProvider.currentUser?.uid?.takeIf { it.isNotBlank() } ?: return
    val device = deviceSyncRepository.getDeviceConfigForUser(currentUid) ?: return
    if (isPushActive.get() || isPullActive.get()) return
    if (outboxDao.getFailedCount() == 0 && _syncState.value != SyncState.ERROR) {
      val now = System.currentTimeMillis()
      val existing = deviceSyncDao.getSyncMetadata(device.deviceId)
      val lastSuccess = existing?.lastSuccessfulSyncAt
      if (lastSuccess == null || now - lastSuccess >= SNAPSHOT_SYNC_TIMESTAMP_THROTTLE_MS) {
        updateSyncMetadata(
          deviceId = device.deviceId,
          state = SyncState.IDLE,
          attemptAt = now,
          successAt = now,
          error = null,
        )
      }
    }
  }

  fun stopRealtimeSync() {
    activeSyncJob?.cancel()
    activeSyncJob = null
    activeShopId = null
    isPushActive.set(false)
    isPullActive.set(false)
  }

  private fun triggerPushOrQueue(ioScope: CoroutineScope) {
    if (pushMutex.isLocked) {
      pushResyncRequested.set(true)
    } else {
      ioScope.launch { pushPendingOutboxNow(forceRetry = false) }
    }
  }

  suspend fun pushPendingOutboxNow(forceRetry: Boolean = false): Result<SyncSummary> =
    withContext(Dispatchers.IO) {
      pushMutex.withLock {
        val currentUid = authSessionProvider.currentUser?.uid?.takeIf { it.isNotBlank() }
          ?: return@withLock Result.failure(IllegalStateException("Sign-In required before syncing."))

        if (!connectivityMonitor.isOnline.value) {
          _syncState.value = SyncState.OFFLINE
          return@withLock Result.failure(IllegalStateException("Device is offline. Changes stay queued safely."))
        }

        val device = deviceSyncRepository.getDeviceConfigForUser(currentUid)
          ?: deviceSyncRepository.ensureLocalDeviceConfig()
        if (device.registrationStatus != DeviceRegistrationStatus.AUTHORIZED) {
          return@withLock Result.failure(IllegalStateException("This device is not authorized for cloud sync."))
        }
        val shopId = device.shopId.takeIf { it.isNotBlank() && it != ShopRepository.DEFAULT_SHOP_ID } ?: currentUid

        isPushActive.set(true)
        _syncState.value = SyncState.SYNCING
        _lastError.value = null
        val now = System.currentTimeMillis()
        updateSyncMetadata(device.deviceId, SyncState.SYNCING, attemptAt = now)

        try {
          outboxDao.resetStaleInProgress()
          if (forceRetry) {
            outboxDao.resetFailedForImmediateRetry(now)
          }

          var assignedTotal = 0
          var totalPushed = 0
          var totalConflicts = 0
          var totalFailed = 0
          var latestFailedMsg: String? = null
          var reservationError: String? = null
          do {
            pushResyncRequested.set(false)
            reservationError = tryEnsureOrderNumberReservationBlock(shopId, device.deviceId)
            assignedTotal += orderRepository.assignPendingOrderNumbers(device.deviceId)
            val stats = processOutbox(shopId, device.deviceId, device.userRole)
            totalPushed += stats.pushed
            totalConflicts += stats.conflicts
            totalFailed += stats.failed
            if (stats.latestError != null) {
              latestFailedMsg = stats.latestError
            }
          } while (pushResyncRequested.getAndSet(false) && connectivityMonitor.isOnline.value)

          isPushActive.set(false)
          val completedAt = System.currentTimeMillis()
          val remainingFailedCount = outboxDao.getFailedCount()
          val persistedFailedError = if (remainingFailedCount > 0) outboxDao.getLatestFailedError() else null
          val unnumberedWaiting = if (reservationError != null) {
            orderDao.getOrdersPendingOfficialNumber().isNotEmpty()
          } else {
            false
          }
          val conflictMsg = if (totalConflicts > 0) "$totalConflicts sync conflict(s) require resolution." else null
          val failureMsg = conflictMsg ?: when {
            remainingFailedCount > 0 || totalFailed > 0 ->
              persistedFailedError
                ?: latestFailedMsg
                ?: "$remainingFailedCount sync item(s) failed. Retrying automatically."
            unnumberedWaiting -> reservationError
            else -> null
          }
          val finishedState = resolveFinishedState(totalConflicts, failureMsg)
          _lastError.value = failureMsg
          updateSyncMetadata(
            deviceId = device.deviceId,
            state = finishedState,
            attemptAt = completedAt,
            successAt = if (failureMsg == null) completedAt else null,
            error = failureMsg,
          )
          _syncState.value = finishedState
          Result.success(
            SyncSummary(
              pushedCount = totalPushed,
              pulledCount = 0,
              assignedOrderNumbers = assignedTotal,
              conflictsDetected = totalConflicts,
            ),
          )
        } catch (e: TimeoutCancellationException) {
          isPushActive.set(false)
          withContext(NonCancellable) { outboxDao.resetStaleInProgress() }
          val msg = "Sync timed out (${e.message ?: "slow network"}). Will retry automatically."
          _lastError.value = msg
          _syncState.value = if (connectivityMonitor.isOnline.value) SyncState.ERROR else SyncState.OFFLINE
          updateSyncMetadata(device.deviceId, _syncState.value, attemptAt = System.currentTimeMillis(), error = msg)
          Result.failure(e)
        } catch (e: CancellationException) {
          isPushActive.set(false)
          withContext(NonCancellable) { outboxDao.resetStaleInProgress() }
          throw e
        } catch (e: Exception) {
          isPushActive.set(false)
          withContext(NonCancellable) { outboxDao.resetStaleInProgress() }
          val msg = e.localizedMessage ?: "Sync failed. Will retry automatically."
          _lastError.value = msg
          _syncState.value = if (connectivityMonitor.isOnline.value) SyncState.ERROR else SyncState.OFFLINE
          updateSyncMetadata(device.deviceId, _syncState.value, attemptAt = System.currentTimeMillis(), error = msg)
          Result.failure(e)
        }
      }
    }

  suspend fun syncNow(forceRetry: Boolean = false): Result<SyncSummary> = withContext(Dispatchers.IO) {
    val pushResult = pushPendingOutboxNow(forceRetry = forceRetry)
    val pushSummary = pushResult.getOrElse { return@withContext Result.failure(it) }

    val currentUid = authSessionProvider.currentUser?.uid?.takeIf { it.isNotBlank() }
      ?: return@withContext Result.failure(IllegalStateException("Sign-In required before syncing."))
    if (!connectivityMonitor.isOnline.value) {
      _syncState.value = SyncState.OFFLINE
      return@withContext Result.failure(IllegalStateException("Device is offline. Changes stay queued safely."))
    }
    val device = deviceSyncRepository.getDeviceConfigForUser(currentUid)
      ?: deviceSyncRepository.ensureLocalDeviceConfig()
    if (device.registrationStatus != DeviceRegistrationStatus.AUTHORIZED) {
      return@withContext Result.failure(IllegalStateException("This device is not authorized for cloud sync."))
    }
    val shopId = device.shopId.takeIf { it.isNotBlank() && it != ShopRepository.DEFAULT_SHOP_ID } ?: currentUid

    val pulled = pullMutex.withLock {
      isPullActive.set(true)
      _syncState.value = SyncState.SYNCING
      try {
        pullAllRemoteCollections(shopId, device.deviceId)
      } catch (e: TimeoutCancellationException) {
        isPullActive.set(false)
        val msg = "Sync timed out (${e.message ?: "slow network"}). Will retry automatically."
        _lastError.value = msg
        _syncState.value = if (connectivityMonitor.isOnline.value) SyncState.ERROR else SyncState.OFFLINE
        updateSyncMetadata(device.deviceId, _syncState.value, attemptAt = System.currentTimeMillis(), error = msg)
        return@withContext Result.failure(e)
      } catch (e: CancellationException) {
        isPullActive.set(false)
        throw e
      } catch (e: Exception) {
        isPullActive.set(false)
        val msg = e.localizedMessage ?: "Sync failed. Will retry automatically."
        _lastError.value = msg
        _syncState.value = if (connectivityMonitor.isOnline.value) SyncState.ERROR else SyncState.OFFLINE
        updateSyncMetadata(device.deviceId, _syncState.value, attemptAt = System.currentTimeMillis(), error = msg)
        return@withContext Result.failure(e)
      } finally {
        isPullActive.set(false)
      }
    }

    var extraPushed = 0
    var extraAssigned = 0
    var extraConflicts = 0
    if (pushResyncRequested.getAndSet(false) && connectivityMonitor.isOnline.value) {
      val followUp = pushPendingOutboxNow(forceRetry = false).getOrNull()
      if (followUp != null) {
        extraPushed = followUp.pushedCount
        extraAssigned = followUp.assignedOrderNumbers
        extraConflicts = followUp.conflictsDetected
      }
    }

    val totalConflicts = pushSummary.conflictsDetected + extraConflicts
    val completedAt = System.currentTimeMillis()
    val remainingFailedCount = outboxDao.getFailedCount()
    val persistedFailedError = if (remainingFailedCount > 0) outboxDao.getLatestFailedError() else null
    val conflictMsg = if (totalConflicts > 0) "$totalConflicts sync conflict(s) require resolution." else null
    val failureMsg = conflictMsg ?: if (remainingFailedCount > 0) {
      persistedFailedError
        ?: _lastError.value
        ?: "$remainingFailedCount sync item(s) failed. Retrying automatically."
    } else {
      null
    }
    val finalState = resolveFinishedState(totalConflicts, failureMsg)
    _lastError.value = failureMsg
    updateSyncMetadata(
      deviceId = device.deviceId,
      state = finalState,
      attemptAt = completedAt,
      successAt = if (failureMsg == null) completedAt else null,
      error = failureMsg,
    )
    _syncState.value = finalState
    Result.success(
      SyncSummary(
        pushedCount = pushSummary.pushedCount + extraPushed,
        pulledCount = pulled,
        assignedOrderNumbers = pushSummary.assignedOrderNumbers + extraAssigned,
        conflictsDetected = totalConflicts,
      ),
    )
  }

  private fun resolveFinishedState(conflicts: Int, errorMsg: String?): SyncState = when {
    !connectivityMonitor.isOnline.value -> SyncState.OFFLINE
    isPushActive.get() || isPullActive.get() -> SyncState.SYNCING
    conflicts > 0 || errorMsg != null -> SyncState.ERROR
    else -> SyncState.IDLE
  }

  suspend fun reserveAdditionalNumberBlock(): Result<OrderNumberReservationEntity> {
    val currentUid = authSessionProvider.currentUser?.uid?.takeIf { it.isNotBlank() }
      ?: return Result.failure(IllegalStateException("Sign-In required."))
    if (!connectivityMonitor.isOnline.value) {
      return Result.failure(IllegalStateException("Internet connection required to reserve order numbers."))
    }
    val device = deviceSyncRepository.getDeviceConfigForUser(currentUid)
      ?: deviceSyncRepository.ensureLocalDeviceConfig()
    if (device.registrationStatus != DeviceRegistrationStatus.AUTHORIZED) {
      return Result.failure(IllegalStateException("Device must be authorized first."))
    }
    val shopId = device.shopId.takeIf { it.isNotBlank() && it != ShopRepository.DEFAULT_SHOP_ID } ?: currentUid
    return try {
      val reserved = reserveBlockTransaction(shopId, device.deviceId)
      orderRepository.assignPendingOrderNumbers(device.deviceId)
      Result.success(reserved)
    } catch (e: CancellationException) {
      throw e
    } catch (e: Exception) {
      handleFirestoreError(e, OperationType.WRITE, "shops/$shopId/order_reservations")
      Result.failure(e)
    }
  }

  suspend fun resolveConflict(
    conflictId: String,
    choice: ConflictResolutionChoice,
  ): Result<Unit> {
    val device = deviceSyncRepository.ensureLocalDeviceConfig()
    val conflict = deviceSyncDao.getConflictById(conflictId)
      ?: return Result.failure(IllegalArgumentException("Conflict not found."))

    return runCatching {
      val now = System.currentTimeMillis()
      database.withTransaction {
        when (choice) {
          ConflictResolutionChoice.KEEP_LOCAL -> {
            val bumpedVersion = maxOf(conflict.localVersion, conflict.remoteVersion) + 1L
            val newPayloadJson = when (conflict.entityType) {
              SyncEntityType.CUSTOMER -> {
                val local = json.decodeFromString<CustomerEntity>(conflict.localPayloadJson)
                  .copy(version = bumpedVersion, updatedAt = now, lastModifiedByDevice = device.deviceId)
                customerDao.update(local)
                json.encodeToString(local)
              }
              SyncEntityType.MEASUREMENT -> {
                val local = json.decodeFromString<MeasurementEntity>(conflict.localPayloadJson)
                  .copy(version = bumpedVersion, updatedAt = now, lastModifiedByDevice = device.deviceId)
                measurementDao.insert(local)
                json.encodeToString(local)
              }
              SyncEntityType.ORDER -> {
                val local = json.decodeFromString<OrderEntity>(conflict.localPayloadJson)
                  .copy(version = bumpedVersion, updatedAt = now, lastModifiedByDevice = device.deviceId)
                orderDao.update(local)
                json.encodeToString(local)
              }
              else -> conflict.localPayloadJson
            }

            val conflictEvent = outboxDao.findConflictEventForEntity(conflict.entityType, conflict.entityId)
            if (conflictEvent != null) {
              outboxDao.updatePayloadAndReset(conflictEvent.eventId, newPayloadJson, now)
            } else {
              outboxDao.enqueue(
                SyncOutboxEntity(
                  eventId = UUID.randomUUID().toString(),
                  entityType = conflict.entityType,
                  entityId = conflict.entityId,
                  operation = SyncOperation.UPDATE,
                  payloadJson = newPayloadJson,
                  createdAt = now,
                ),
              )
            }
          }

          ConflictResolutionChoice.USE_CLOUD -> {
            when (conflict.entityType) {
              SyncEntityType.CUSTOMER -> {
                val remote = json.decodeFromString<CustomerEntity>(conflict.remotePayloadJson)
                customerDao.insert(remote)
              }
              SyncEntityType.MEASUREMENT -> {
                val remote = json.decodeFromString<MeasurementEntity>(conflict.remotePayloadJson)
                if (remote.isCurrent) {
                  measurementDao.clearCurrentFlagExcept(remote.customerId, remote.measurementId, now)
                }
                measurementDao.insert(remote)
              }
              SyncEntityType.ORDER -> {
                val remote = json.decodeFromString<OrderEntity>(conflict.remotePayloadJson)
                orderDao.insert(remote)
              }
              else -> Unit
            }
            val conflictEvent = outboxDao.findConflictEventForEntity(conflict.entityType, conflict.entityId)
            if (conflictEvent != null) {
              outboxDao.updateStatus(conflictEvent.eventId, SyncStatus.SYNCED, null)
            }
          }
        }
        deviceSyncDao.markConflictResolved(conflictId, now)
      }

      if (choice == ConflictResolutionChoice.KEEP_LOCAL && connectivityMonitor.isOnline.value) {
        pushPendingOutboxNow(forceRetry = false)
      }
    }
  }

  private fun observeRemoteCollection(
    shopId: String,
    collection: String,
    ioScope: CoroutineScope,
    onDocuments: suspend (List<DocumentSnapshot>) -> Unit,
  ) {
    val path = "shops/$shopId/$collection"
    var initialProcessed = false
    db.collection("shops").document(shopId).collection(collection)
      .snapshots()
      .onEach { snap ->
        val docs = if (!initialProcessed) {
          initialProcessed = true
          snap.documents
        } else {
          snap.documentChanges
            .filter { it.type != DocumentChange.Type.REMOVED }
            .map { it.document }
        }
        if (docs.isNotEmpty()) {
          onDocuments(docs)
        }
      }
      .flowOn(Dispatchers.IO)
      .catch { error ->
        val firestoreError = generateSequence(error) { it.cause }
          .filterIsInstance<FirebaseFirestoreException>()
          .firstOrNull()
        if (firestoreError == null && error is CancellationException) throw error
        val rootError = firestoreError ?: error
        if (authSessionProvider.currentUser != null) {
          handleFirestoreError(rootError, OperationType.LIST, path)
        }
      }
      .launchIn(ioScope)
  }

  private suspend fun fetchMissingCustomers(
    remoteShopId: String,
    localShopId: String,
    customerIds: List<String>,
  ): List<CustomerEntity> {
    if (customerIds.isEmpty()) return emptyList()
    return coroutineScope {
      customerIds.distinct().map { id ->
        async {
          runCatching {
            withTimeout(PARENT_FETCH_TIMEOUT_MS) {
              val snap = db.collection("shops").document(remoteShopId)
                .collection("customers").document(id).get().await()
              if (snap.exists()) {
                snap.toObject(CustomerDoc::class.java, DocumentSnapshot.ServerTimestampBehavior.ESTIMATE)
                  ?.toLocalEntity(localShopId)
              } else {
                null
              }
            }
          }.getOrNull()
        }
      }.awaitAll().filterNotNull()
    }
  }

  private suspend fun fetchMissingOrders(
    remoteShopId: String,
    localShopId: String,
    orderIds: List<String>,
  ): List<OrderEntity> {
    if (orderIds.isEmpty()) return emptyList()
    return coroutineScope {
      orderIds.distinct().map { id ->
        async {
          runCatching {
            withTimeout(PARENT_FETCH_TIMEOUT_MS) {
              val snap = db.collection("shops").document(remoteShopId)
                .collection("orders").document(id).get().await()
              if (snap.exists()) {
                snap.toObject(OrderDoc::class.java, DocumentSnapshot.ServerTimestampBehavior.ESTIMATE)
                  ?.toLocalEntity(localShopId)
              } else {
                null
              }
            }
          }.getOrNull()
        }
      }.awaitAll().filterNotNull()
    }
  }

  private suspend fun tryEnsureOrderNumberReservationBlock(uid: String, deviceId: String): String? {
    return try {
      ensureOrderNumberReservationBlock(uid, deviceId)
      null
    } catch (e: TimeoutCancellationException) {
      Log.w("SyncEngine", "Order number block check timed out; continuing outbox sync: ${e.message}")
      "Order number reservation timed out (${e.message ?: "slow network"}). Will retry automatically."
    } catch (e: CancellationException) {
      throw e
    } catch (e: Exception) {
      Log.w("SyncEngine", "Order number block reservation failed; continuing outbox sync: ${e.message}")
      formatPushError(e)
    }
  }

  private suspend fun ensureOrderNumberReservationBlock(uid: String, deviceId: String) {
    val existingActive = deviceSyncDao.getActiveReservationsForDevice(deviceId)
    val remaining = existingActive.sumOf { (it.endNumber - it.nextAvailableNumber + 1).coerceAtLeast(0) }
    if (remaining >= MIN_NUMBER_BUFFER) return

    val remoteResSnap = withTimeout(RESERVATION_TIMEOUT_MS) {
      db.collection("shops").document(uid).collection("order_reservations")
        .whereEqualTo("deviceId", deviceId)
        .get()
        .await()
    }
    val remoteList = remoteResSnap.toObjects(
      OrderReservationDoc::class.java,
      DocumentSnapshot.ServerTimestampBehavior.ESTIMATE,
    ).map { it.toLocalEntity() }
    remoteApplier.applyRemoteReservations(remoteList)

    val refreshedActive = deviceSyncDao.getActiveReservationsForDevice(deviceId)
    val refreshedRemaining = refreshedActive.sumOf { (it.endNumber - it.nextAvailableNumber + 1).coerceAtLeast(0) }
    if (refreshedRemaining < MIN_NUMBER_BUFFER) {
      reserveBlockTransaction(uid, deviceId)
    }
  }

  private suspend fun reserveBlockTransaction(
    uid: String,
    deviceId: String,
  ): OrderNumberReservationEntity {
    val shopRef = db.collection("shops").document(uid)
    val reservationId = UUID.randomUUID().toString()
    val resRef = shopRef.collection("order_reservations").document(reservationId)

    val (startNum, endNum) = withTimeout(RESERVATION_TIMEOUT_MS) {
      db.runTransaction { tx ->
        val shopSnap = tx.get(shopRef)
        val currentNext = (shopSnap.getLong("nextOrderNumber") ?: 1001L).coerceAtLeast(1001L)
        val start = currentNext
        val end = start + ORDER_BLOCK_SIZE - 1L
        tx.update(
          shopRef,
          mapOf(
            "nextOrderNumber" to (end + 1L),
            "updatedAt" to FieldValue.serverTimestamp(),
          ),
        )
        tx.set(
          resRef,
          mapOf(
            "ownerUid" to uid,
            "deviceId" to deviceId,
            "startNumber" to start,
            "endNumber" to end,
            "createdAt" to FieldValue.serverTimestamp(),
            "updatedAt" to FieldValue.serverTimestamp(),
          ),
        )
        start.toInt() to end.toInt()
      }.await()
    }

    return deviceSyncRepository.addOrderNumberReservation(
      deviceId = deviceId,
      startNumber = startNum,
      endNumber = endNum,
      reservationId = reservationId,
    )
  }

  private data class OutboxProcessStats(
    val pushed: Int = 0,
    val conflicts: Int = 0,
    val failed: Int = 0,
    val latestError: String? = null,
  )

  private sealed interface OutboxPushOutcome {
    data class Succeeded(val event: SyncOutboxEntity) : OutboxPushOutcome
    data class Conflicted(val event: SyncOutboxEntity) : OutboxPushOutcome
    data class Failed(val event: SyncOutboxEntity, val error: String) : OutboxPushOutcome
  }

  private suspend fun processOutbox(
    uid: String,
    localDeviceId: String,
    userRole: String = ShopUserRole.OWNER.name,
  ): OutboxProcessStats {
    var pushed = 0
    var conflicts = 0
    var failed = 0
    var latestError: String? = null
    var wroteOrderSyncLogs = false
    val attemptedEventIds = HashSet<String>()

    while (connectivityMonitor.isOnline.value) {
      val ready = outboxDao.getReadyEvents(System.currentTimeMillis(), OUTBOX_BATCH_FETCH_SIZE)
        .filter { it.eventId !in attemptedEventIds }
      if (ready.isEmpty()) break

      val tiers = ready
        .sortedWith(compareBy({ dependencyPriority(it.entityType) }, { it.createdAt }))
        .groupBy { dependencyPriority(it.entityType) }
        .toSortedMap()

      for ((priority, tierEvents) in tiers) {
        if (!connectivityMonitor.isOnline.value) break

        // Coalesce multiple active events for the same entityId so only the latest snapshot is pushed sequentially
        val latestPerEntity = ArrayList<SyncOutboxEntity>()
        for ((_, entityGroup) in tierEvents.groupBy { it.entityId }) {
          val sorted = entityGroup.sortedBy { it.createdAt }
          attemptedEventIds.addAll(sorted.map { it.eventId })
          latestPerEntity.add(sorted.last())
        }

        val blockedParentIds: Set<String> = when (priority) {
          dependencyPriority(SyncEntityType.MEASUREMENT),
          dependencyPriority(SyncEntityType.ORDER) ->
            outboxDao.getEntityIdsWithUnresolvedCreate(SyncEntityType.CUSTOMER).toHashSet()
          dependencyPriority(SyncEntityType.PAYMENT) ->
            outboxDao.getEntityIdsWithUnresolvedCreate(SyncEntityType.ORDER).toHashSet()
          else -> emptySet()
        }

        val unblockedEvents = if (blockedParentIds.isEmpty()) {
          latestPerEntity
        } else {
          val unblocked = ArrayList<SyncOutboxEntity>(latestPerEntity.size)
          val deferredFailedIds = ArrayList<String>()
          for (event in latestPerEntity) {
            val parentId = extractParentId(event)
            if (parentId != null && parentId in blockedParentIds) {
              if (event.syncStatus == SyncStatus.FAILED) {
                deferredFailedIds.add(event.eventId)
              }
            } else {
              unblocked.add(event)
            }
          }
          if (deferredFailedIds.isNotEmpty()) {
            outboxDao.resetToPendingBatch(deferredFailedIds)
          }
          unblocked
        }

        for (chunk in unblockedEvents.chunked(MAX_CONCURRENT_WRITES)) {
          if (!connectivityMonitor.isOnline.value) break
          val chunkIds = chunk.map { it.eventId }
          outboxDao.markInProgressBatch(chunkIds)

          val outcomes = try {
            coroutineScope {
              chunk.map { event ->
                async {
                  pushSingleEvent(uid, localDeviceId, userRole, event)
                }
              }.awaitAll()
            }
          } catch (e: CancellationException) {
            withContext(NonCancellable) {
              outboxDao.resetToPendingBatch(chunkIds)
            }
            throw e
          }

          for (outcome in outcomes) {
            when (outcome) {
              is OutboxPushOutcome.Succeeded -> {
                val ev = outcome.event
                outboxDao.markSyncedForEntityUpTo(ev.entityType, ev.entityId, ev.createdAt)
                pushed += 1
                if (ev.entityType == SyncEntityType.ORDER || ev.entityType == SyncEntityType.PAYMENT) {
                  wroteOrderSyncLogs = true
                }
              }
              is OutboxPushOutcome.Conflicted -> {
                conflicts += 1
                if (outcome.event.entityType == SyncEntityType.ORDER) {
                  wroteOrderSyncLogs = true
                }
              }
              is OutboxPushOutcome.Failed -> {
                val ev = outcome.event
                val nextRetryAt = System.currentTimeMillis() + ConflictResolver.computeBackoffMillis(ev.retryCount)
                outboxDao.markRetry(
                  eventId = ev.eventId,
                  nextRetryAt = nextRetryAt,
                  error = outcome.error,
                )
                failed += 1
                latestError = outcome.error
                if (ev.entityType == SyncEntityType.ORDER || ev.entityType == SyncEntityType.PAYMENT) {
                  logOrderOrPaymentFailure(ev, localDeviceId, outcome.error)
                  wroteOrderSyncLogs = true
                }
              }
            }
          }
        }
      }
    }
    if (wroteOrderSyncLogs) {
      orderSyncLogDao.pruneExcessLogs()
    }
    return OutboxProcessStats(
      pushed = pushed,
      conflicts = conflicts,
      failed = failed,
      latestError = latestError,
    )
  }

  private fun extractParentId(event: SyncOutboxEntity): String? = runCatching {
    when (event.entityType) {
      SyncEntityType.MEASUREMENT -> json.decodeFromString<MeasurementEntity>(event.payloadJson).customerId
      SyncEntityType.ORDER -> json.decodeFromString<OrderEntity>(event.payloadJson).customerId
      SyncEntityType.PAYMENT -> json.decodeFromString<PaymentEntity>(event.payloadJson).orderId
      else -> null
    }
  }.getOrNull()

  private suspend fun pushSingleEvent(
    uid: String,
    localDeviceId: String,
    userRole: String,
    event: SyncOutboxEntity,
  ): OutboxPushOutcome {
    val path = "shops/$uid"
    return try {
      when (event.entityType) {
        SyncEntityType.SHOP -> {
          if (userRole == ShopUserRole.OWNER.name) pushShopEvent(uid, event)
          OutboxPushOutcome.Succeeded(event)
        }
        SyncEntityType.CUSTOMER -> {
          if (pushCustomerEvent(uid, localDeviceId, event)) {
            OutboxPushOutcome.Conflicted(event)
          } else {
            OutboxPushOutcome.Succeeded(event)
          }
        }
        SyncEntityType.MEASUREMENT -> {
          if (pushMeasurementEvent(uid, localDeviceId, event)) {
            OutboxPushOutcome.Conflicted(event)
          } else {
            OutboxPushOutcome.Succeeded(event)
          }
        }
        SyncEntityType.ORDER -> {
          if (pushOrderEvent(uid, localDeviceId, event)) {
            OutboxPushOutcome.Conflicted(event)
          } else {
            OutboxPushOutcome.Succeeded(event)
          }
        }
        SyncEntityType.PAYMENT -> {
          pushPaymentEvent(uid, localDeviceId, event)
          OutboxPushOutcome.Succeeded(event)
        }
        SyncEntityType.DEVICE_CONFIG, SyncEntityType.ORDER_RESERVATION ->
          OutboxPushOutcome.Succeeded(event)
      }
    } catch (e: TimeoutCancellationException) {
      handleFirestoreError(e, OperationType.WRITE, "$path/${event.entityType.name.lowercase()}/${event.entityId}")
      OutboxPushOutcome.Failed(
        event = event,
        error = "Upload timed out; retrying automatically.",
      )
    } catch (e: CancellationException) {
      throw e
    } catch (e: Exception) {
      handleFirestoreError(e, OperationType.WRITE, "$path/${event.entityType.name.lowercase()}/${event.entityId}")
      OutboxPushOutcome.Failed(
        event = event,
        error = formatPushError(e),
      )
    }
  }

  private fun formatPushError(e: Exception): String {
    val fsEx = generateSequence<Throwable>(e) { it.cause }
      .filterIsInstance<FirebaseFirestoreException>()
      .firstOrNull()
    return if (fsEx != null) {
      val detail = fsEx.localizedMessage ?: fsEx.message ?: "Firestore write rejected"
      "[${fsEx.code.name}] $detail"
    } else {
      e.localizedMessage ?: e.message ?: "Push failed"
    }
  }

  private suspend fun pushShopEvent(uid: String, event: SyncOutboxEntity) {
    val shop = json.decodeFromString<ShopEntity>(event.payloadJson)
    val docRef = db.collection("shops").document(uid)
    withTimeout(WRITE_TIMEOUT_MS) {
      val snap = docRef.get().await()
      if (snap.exists()) {
        docRef.update(
          mapOf(
            "shopName" to shop.shopName.trim().take(80).ifEmpty { "Darzi Ledger" },
            "contactNumber" to shop.contactNumber.trim().take(25),
            "email" to shop.email.trim().take(120),
            "version" to shop.version.coerceAtLeast(1L),
            "updatedAt" to FieldValue.serverTimestamp(),
          ),
        ).await()
      } else {
        docRef.set(
          mapOf(
            "ownerUid" to uid,
            "shopName" to shop.shopName.trim().take(80).ifEmpty { "Darzi Ledger" },
            "contactNumber" to shop.contactNumber.trim().take(25),
            "email" to shop.email.trim().take(120),
            "nextOrderNumber" to 1001L,
            "version" to shop.version.coerceAtLeast(1L),
            "createdAt" to FieldValue.serverTimestamp(),
            "updatedAt" to FieldValue.serverTimestamp(),
          ),
        ).await()
      }
    }
  }

  private suspend fun pushCustomerEvent(
    uid: String,
    localDeviceId: String,
    event: SyncOutboxEntity,
  ): Boolean {
    val decoded = json.decodeFromString<CustomerEntity>(event.payloadJson)
    val local = if (decoded.lastModifiedByDevice != localDeviceId) {
      decoded.copy(lastModifiedByDevice = localDeviceId)
    } else {
      decoded
    }
    val docRef = db.collection("shops").document(uid).collection("customers").document(local.customerId)
    val remoteSnap = withTimeout(WRITE_TIMEOUT_MS) { docRef.get().await() }

    if (!remoteSnap.exists()) {
      withTimeout(WRITE_TIMEOUT_MS) {
        docRef.set(SyncMappers.customerToCreateMap(uid, local)).await()
      }
      return false
    }

    val remoteDoc = remoteSnap.toObject(CustomerDoc::class.java, DocumentSnapshot.ServerTimestampBehavior.ESTIMATE)
    if (remoteDoc != null && ConflictResolver.hasConcurrentUpdateConflict(
        localVersion = local.version,
        localDeviceId = localDeviceId,
        remoteVersion = remoteDoc.version,
        remoteDeviceId = remoteDoc.lastModifiedByDevice,
      )
    ) {
      val conflict = ConflictResolver.createCustomerConflict(
        local = local,
        remote = remoteDoc.toLocalEntity(local.shopId),
      )
      deviceSyncDao.upsertConflict(conflict)
      outboxDao.markConflict(event.eventId, "Customer edited concurrently on another device.")
      return true
    }

    if (remoteDoc != null && remoteDoc.lastModifiedByDevice == localDeviceId && remoteDoc.version > local.version) {
      return false
    }

    withTimeout(WRITE_TIMEOUT_MS) {
      docRef.update(SyncMappers.customerToUpdateMap(local)).await()
    }
    return false
  }

  private suspend fun pushMeasurementEvent(
    uid: String,
    localDeviceId: String,
    event: SyncOutboxEntity,
  ): Boolean {
    val decoded = json.decodeFromString<MeasurementEntity>(event.payloadJson)
    val local = if (decoded.lastModifiedByDevice != localDeviceId) {
      decoded.copy(lastModifiedByDevice = localDeviceId)
    } else {
      decoded
    }
    val docRef = db.collection("shops").document(uid).collection("measurements").document(local.measurementId)
    val remoteSnap = withTimeout(WRITE_TIMEOUT_MS) { docRef.get().await() }

    if (!remoteSnap.exists()) {
      withTimeout(WRITE_TIMEOUT_MS) {
        docRef.set(SyncMappers.measurementToCreateMap(uid, local)).await()
      }
      return false
    }

    val remoteDoc = remoteSnap.toObject(MeasurementDoc::class.java, DocumentSnapshot.ServerTimestampBehavior.ESTIMATE)
    if (remoteDoc != null && ConflictResolver.hasConcurrentUpdateConflict(
        localVersion = local.version,
        localDeviceId = localDeviceId,
        remoteVersion = remoteDoc.version,
        remoteDeviceId = remoteDoc.lastModifiedByDevice,
      )
    ) {
      val conflict = ConflictResolver.createMeasurementConflict(
        local = local,
        remote = remoteDoc.toLocalEntity(),
      )
      deviceSyncDao.upsertConflict(conflict)
      outboxDao.markConflict(event.eventId, "Measurement edited concurrently on another device.")
      return true
    }

    if (remoteDoc != null && remoteDoc.lastModifiedByDevice == localDeviceId && remoteDoc.version > local.version) {
      return false
    }

    withTimeout(WRITE_TIMEOUT_MS) {
      docRef.update(SyncMappers.measurementToUpdateMap(local)).await()
    }
    return false
  }

  private suspend fun pushOrderEvent(
    uid: String,
    localDeviceId: String,
    event: SyncOutboxEntity,
  ): Boolean {
    val decoded = json.decodeFromString<OrderEntity>(event.payloadJson)
    val local = if (decoded.lastModifiedByDevice != localDeviceId) {
      decoded.copy(lastModifiedByDevice = localDeviceId)
    } else {
      decoded
    }
    val logOp = resolveOrderLogOperation(event, local)
    recordOrderSyncLog(
      orderId = local.orderId,
      orderNumber = local.orderNumber,
      operation = logOp,
      status = OrderSyncLogStatus.ATTEMPTING,
      summary = "Syncing v${local.version} (${local.orderStatus.name}, ${local.paymentStatus.name}, deleted=${local.isDeleted}) to cloud",
      deviceId = localDeviceId,
      orderVersion = local.version,
    )

    val docRef = db.collection("shops").document(uid).collection("orders").document(local.orderId)
    val remoteSnap = withTimeout(WRITE_TIMEOUT_MS) { docRef.get().await() }

    if (!remoteSnap.exists()) {
      withTimeout(WRITE_TIMEOUT_MS) {
        docRef.set(SyncMappers.orderToCreateMap(uid, local)).await()
      }
      recordOrderSyncLog(
        orderId = local.orderId,
        orderNumber = local.orderNumber,
        operation = OrderSyncLogOperation.CREATE,
        status = OrderSyncLogStatus.SUCCEEDED,
        summary = "Created cloud order (${local.orderStatus.name}, ${local.paymentStatus.name}, Rs. ${local.paidAmountPkr}/${local.totalAmountPkr})",
        deviceId = localDeviceId,
        orderVersion = local.version,
      )
      return false
    }

    val remoteDoc = remoteSnap.toObject(OrderDoc::class.java, DocumentSnapshot.ServerTimestampBehavior.ESTIMATE)
    if (remoteDoc != null && ConflictResolver.hasConcurrentUpdateConflict(
        localVersion = local.version,
        localDeviceId = localDeviceId,
        remoteVersion = remoteDoc.version,
        remoteDeviceId = remoteDoc.lastModifiedByDevice,
      )
    ) {
      val conflict = ConflictResolver.createOrderConflict(
        local = local,
        remote = remoteDoc.toLocalEntity(local.shopId),
      )
      deviceSyncDao.upsertConflict(conflict)
      outboxDao.markConflict(event.eventId, "Order edited concurrently on another device.")
      recordOrderSyncLog(
        orderId = local.orderId,
        orderNumber = local.orderNumber,
        operation = logOp,
        status = OrderSyncLogStatus.CONFLICT,
        summary = "Conflict detected: local v${local.version} vs cloud v${remoteDoc.version}",
        errorMessage = "Edited concurrently on device ${remoteDoc.lastModifiedByDevice}",
        deviceId = localDeviceId,
        orderVersion = local.version,
      )
      return true
    }

    if (remoteDoc == null) {
      withTimeout(WRITE_TIMEOUT_MS) {
        docRef.update(
          SyncMappers.orderDetailsUpdateMap(
            entity = local,
            includeOrderNumber = local.orderNumber != null,
          ),
        ).await()
      }
      recordOrderSyncLog(
        orderId = local.orderId,
        orderNumber = local.orderNumber,
        operation = logOp,
        status = OrderSyncLogStatus.SUCCEEDED,
        summary = "Synced order details (${local.orderStatus.name}, deleted=${local.isDeleted})",
        deviceId = localDeviceId,
        orderVersion = local.version,
      )
      return false
    }

    if (remoteDoc.lastModifiedByDevice == localDeviceId && remoteDoc.version > local.version) {
      recordOrderSyncLog(
        orderId = local.orderId,
        orderNumber = local.orderNumber,
        operation = logOp,
        status = OrderSyncLogStatus.SKIPPED,
        summary = "Skipped stale local v${local.version}; cloud already has v${remoteDoc.version} from this device",
        deviceId = localDeviceId,
        orderVersion = local.version,
      )
      return false
    }

    val remotePaid = remoteDoc.paidAmountPkr
    val remoteStatus = remoteDoc.orderStatus
    val remoteNotes = remoteDoc.notes
    val remoteNumber = remoteDoc.orderNumber?.toInt()
    val remoteSuitCount = remoteDoc.suitCount.toInt()
    val remoteDeliveryDate = remoteDoc.deliveryDateMillis

    val paymentChanged = local.paidAmountPkr != remotePaid ||
      local.remainingAmountPkr != remoteDoc.remainingAmountPkr ||
      local.paymentStatus.name != remoteDoc.paymentStatus
    val detailsChanged = local.orderStatus.name != remoteStatus ||
      local.notes != remoteNotes ||
      local.suitCount != remoteSuitCount ||
      local.deliveryDate != remoteDeliveryDate ||
      local.isDeleted != remoteDoc.deleted ||
      (local.orderNumber != null && remoteNumber == null)

    val pushedActions = ArrayList<String>(2)
    withTimeout(WRITE_TIMEOUT_MS) {
      if (paymentChanged) {
        docRef.update(SyncMappers.orderPaymentUpdateMap(local)).await()
        pushedActions.add("payment=${local.paymentStatus.name} (Rs. ${local.paidAmountPkr}/${local.totalAmountPkr})")
      }
      if (detailsChanged || !paymentChanged) {
        docRef.update(
          SyncMappers.orderDetailsUpdateMap(
            entity = local,
            includeOrderNumber = local.orderNumber != null && remoteNumber == null,
          ),
        ).await()
        val detailLabel = buildString {
          append("status=${local.orderStatus.name}")
          if (local.isDeleted) append(", deleted=true")
          if (local.orderNumber != null && remoteNumber == null) append(", #${local.orderNumber}")
        }
        pushedActions.add(detailLabel)
      }
    }
    recordOrderSyncLog(
      orderId = local.orderId,
      orderNumber = local.orderNumber,
      operation = logOp,
      status = OrderSyncLogStatus.SUCCEEDED,
      summary = "Synced to cloud: ${pushedActions.joinToString("; ")}",
      deviceId = localDeviceId,
      orderVersion = local.version,
    )
    return false
  }

  private suspend fun pushPaymentEvent(
    uid: String,
    localDeviceId: String,
    event: SyncOutboxEntity,
  ) {
    val decoded = json.decodeFromString<PaymentEntity>(event.payloadJson)
    val local = if (decoded.deviceId != localDeviceId) {
      decoded.copy(deviceId = localDeviceId)
    } else {
      decoded
    }
    val docRef = db.collection("shops").document(uid).collection("payments").document(local.paymentId)
    withTimeout(WRITE_TIMEOUT_MS) {
      val existing = docRef.get().await()
      if (!existing.exists()) {
        docRef.set(SyncMappers.paymentToCreateMap(uid, local)).await()
        val orderNum = orderDao.getById(local.orderId)?.orderNumber
        recordOrderSyncLog(
          orderId = local.orderId,
          orderNumber = orderNum,
          operation = OrderSyncLogOperation.PAYMENT,
          status = OrderSyncLogStatus.SUCCEEDED,
          summary = "Uploaded payment receipt Rs. ${local.amountPkr} (${local.paymentMethod.name})",
          deviceId = localDeviceId,
          orderVersion = 1L,
        )
      }
    }
  }

  private suspend fun logOrderOrPaymentFailure(
    event: SyncOutboxEntity,
    localDeviceId: String,
    error: String,
  ) {
    when (event.entityType) {
      SyncEntityType.ORDER -> {
        val order = runCatching { json.decodeFromString<OrderEntity>(event.payloadJson) }.getOrNull()
        recordOrderSyncLog(
          orderId = event.entityId,
          orderNumber = order?.orderNumber,
          operation = resolveOrderLogOperation(event, order),
          status = OrderSyncLogStatus.FAILED,
          summary = "Cloud push failed (retry #${event.retryCount + 1}) for ${order?.orderStatus?.name ?: "ORDER"}/${order?.paymentStatus?.name ?: ""}",
          errorMessage = error,
          deviceId = localDeviceId,
          orderVersion = order?.version ?: 1L,
        )
      }
      SyncEntityType.PAYMENT -> {
        val payment = runCatching { json.decodeFromString<PaymentEntity>(event.payloadJson) }.getOrNull()
        val order = payment?.let { orderDao.getById(it.orderId) }
        recordOrderSyncLog(
          orderId = payment?.orderId ?: event.entityId,
          orderNumber = order?.orderNumber,
          operation = OrderSyncLogOperation.PAYMENT,
          status = OrderSyncLogStatus.FAILED,
          summary = "Payment receipt push failed (retry #${event.retryCount + 1}) for Rs. ${payment?.amountPkr ?: 0}",
          errorMessage = error,
          deviceId = localDeviceId,
          orderVersion = order?.version ?: 1L,
        )
      }
      else -> Unit
    }
  }

  private fun resolveOrderLogOperation(event: SyncOutboxEntity, order: OrderEntity?): String = when {
    event.operation == SyncOperation.CREATE -> OrderSyncLogOperation.CREATE
    event.operation == SyncOperation.DELETE || order?.isDeleted == true -> OrderSyncLogOperation.DELETE
    else -> OrderSyncLogOperation.UPDATE
  }

  private suspend fun recordOrderSyncLog(
    orderId: String,
    orderNumber: Int?,
    operation: String,
    status: String,
    summary: String,
    errorMessage: String? = null,
    deviceId: String,
    orderVersion: Long,
  ) {
    orderSyncLogDao.insert(
      OrderSyncLogEntity(
        logId = UUID.randomUUID().toString(),
        orderId = orderId,
        orderNumber = orderNumber,
        operation = operation,
        status = status,
        summary = summary,
        errorMessage = errorMessage,
        deviceId = deviceId,
        orderVersion = orderVersion,
        timestamp = System.currentTimeMillis(),
      ),
    )
    runCatching {
      val label = orderNumber?.toString() ?: orderId
      val line = "[$status][$operation] order=$label v$orderVersion: $summary${if (errorMessage != null) " | error=$errorMessage" else ""}"
      if (status == OrderSyncLogStatus.FAILED || status == OrderSyncLogStatus.CONFLICT) {
        Log.e("OrderSyncLog", line)
      } else {
        Log.i("OrderSyncLog", line)
      }
    }
  }

  private suspend fun pullAllRemoteCollections(uid: String, localDeviceId: String): Int {
    val shopId = shopRepository.ensureDefaultShop().shopId
    val shopDocRef = db.collection("shops").document(uid)
    val device = deviceSyncDao.getDeviceConfig() ?: return 0
    val metadata = deviceSyncDao.getSyncMetadata(device.deviceId) ?: SyncMetadataEntity(deviceId = device.deviceId)

    suspend fun queryCollection(name: String, cursor: Long, deviceScoped: Boolean = false): com.google.firebase.firestore.QuerySnapshot {
      var query: com.google.firebase.firestore.Query = shopDocRef.collection(name)
      if (deviceScoped) query = query.whereEqualTo("deviceId", localDeviceId)
      if (cursor > 0L) {
        query = query.whereGreaterThan("updatedAt", Timestamp(Date((cursor - SYNC_CURSOR_OVERLAP_MS).coerceAtLeast(0L))))
      }
      return query.get().await()
    }

    var totalPulled = 0
    withTimeout(PULL_TIMEOUT_MS) {
      val custSnap = queryCollection("customers", metadata.customersLastPulledAt)
      val customers = custSnap.documents.mapNotNull { it.toObject(CustomerDoc::class.java, DocumentSnapshot.ServerTimestampBehavior.ESTIMATE)?.toLocalEntity(shopId) }
      totalPulled += remoteApplier.applyRemoteCustomers(customers)
      persistCollectionCursor(device.deviceId, "customers", maxUpdatedAt(custSnap.documents, metadata.customersLastPulledAt))

      val measSnap = queryCollection("measurements", metadata.measurementsLastPulledAt)
      val measurements = measSnap.documents.mapNotNull { it.toObject(MeasurementDoc::class.java, DocumentSnapshot.ServerTimestampBehavior.ESTIMATE)?.toLocalEntity() }
      totalPulled += remoteApplier.applyRemoteMeasurements(
        remoteList = measurements,
        fetchMissingCustomers = { ids -> fetchMissingCustomers(uid, shopId, ids) },
      )
      persistCollectionCursor(device.deviceId, "measurements", maxUpdatedAt(measSnap.documents, metadata.measurementsLastPulledAt))

      val orderSnap = queryCollection("orders", metadata.ordersLastPulledAt)
      val orders = orderSnap.documents.mapNotNull { it.toObject(OrderDoc::class.java, DocumentSnapshot.ServerTimestampBehavior.ESTIMATE)?.toLocalEntity(shopId) }
      totalPulled += remoteApplier.applyRemoteOrders(
        remoteList = orders,
        fetchMissingCustomers = { ids -> fetchMissingCustomers(uid, shopId, ids) },
      )
      persistCollectionCursor(device.deviceId, "orders", maxUpdatedAt(orderSnap.documents, metadata.ordersLastPulledAt))

      val paymentSnap = queryCollection("payments", metadata.paymentsLastPulledAt)
      val payments = paymentSnap.documents.mapNotNull { it.toObject(PaymentDoc::class.java, DocumentSnapshot.ServerTimestampBehavior.ESTIMATE)?.toLocalEntity() }
      totalPulled += remoteApplier.applyRemotePayments(
        remoteList = payments,
        fetchMissingOrders = { ids -> fetchMissingOrders(uid, shopId, ids) },
        fetchMissingCustomers = { ids -> fetchMissingCustomers(uid, shopId, ids) },
      )
      persistCollectionCursor(device.deviceId, "payments", maxUpdatedAt(paymentSnap.documents, metadata.paymentsLastPulledAt))

      val reservationSnap = queryCollection("order_reservations", metadata.reservationsLastPulledAt, deviceScoped = true)
      val reservations = reservationSnap.documents.mapNotNull { it.toObject(OrderReservationDoc::class.java, DocumentSnapshot.ServerTimestampBehavior.ESTIMATE)?.toLocalEntity() }
      remoteApplier.applyRemoteReservations(reservations)
      persistCollectionCursor(device.deviceId, "order_reservations", maxUpdatedAt(reservationSnap.documents, metadata.reservationsLastPulledAt))
    }
    return totalPulled
  }

  private fun maxUpdatedAt(documents: List<DocumentSnapshot>, fallback: Long): Long =
    documents.mapNotNull { it.getTimestamp("updatedAt")?.toDate()?.time }.maxOrNull() ?: maxOf(fallback, System.currentTimeMillis())

  private suspend fun persistCollectionCursor(deviceId: String, collection: String, cursor: Long) {
    val current = deviceSyncDao.getSyncMetadata(deviceId) ?: SyncMetadataEntity(deviceId = deviceId)
    val updated = when (collection) {
      "customers" -> current.copy(customersLastPulledAt = maxOf(current.customersLastPulledAt, cursor))
      "measurements" -> current.copy(measurementsLastPulledAt = maxOf(current.measurementsLastPulledAt, cursor))
      "orders" -> current.copy(ordersLastPulledAt = maxOf(current.ordersLastPulledAt, cursor))
      "payments" -> current.copy(paymentsLastPulledAt = maxOf(current.paymentsLastPulledAt, cursor))
      "order_reservations" -> current.copy(reservationsLastPulledAt = maxOf(current.reservationsLastPulledAt, cursor))
      else -> current
    }
    deviceSyncDao.upsertSyncMetadata(updated)
  }

  private data class PullSnapshots(
    val custSnap: com.google.firebase.firestore.QuerySnapshot,
    val measSnap: com.google.firebase.firestore.QuerySnapshot,
    val orderSnap: com.google.firebase.firestore.QuerySnapshot,
    val paySnap: com.google.firebase.firestore.QuerySnapshot,
    val resSnap: com.google.firebase.firestore.QuerySnapshot,
  )

  private suspend fun updateSyncMetadata(
    deviceId: String,
    state: SyncState,
    attemptAt: Long,
    successAt: Long? = null,
    error: String? = null,
  ) {
    val current = deviceSyncDao.getSyncMetadata(deviceId) ?: SyncMetadataEntity(deviceId = deviceId)
    deviceSyncDao.upsertSyncMetadata(
      current.copy(
        syncStatus = state,
        lastSyncAttemptAt = attemptAt,
        lastSuccessfulSyncAt = successAt ?: current.lastSuccessfulSyncAt,
        lastError = error,
      ),
    )
  }

  private fun dependencyPriority(type: SyncEntityType): Int = when (type) {
    SyncEntityType.SHOP -> 0
    SyncEntityType.CUSTOMER -> 1
    SyncEntityType.MEASUREMENT -> 2
    SyncEntityType.ORDER -> 3
    SyncEntityType.PAYMENT -> 4
    SyncEntityType.DEVICE_CONFIG, SyncEntityType.ORDER_RESERVATION -> 5
  }

  companion object {
    const val ORDER_BLOCK_SIZE = 50L
    const val MIN_NUMBER_BUFFER = 5
    const val WRITE_TIMEOUT_MS = 45_000L
    const val RESERVATION_TIMEOUT_MS = 45_000L
    const val PULL_TIMEOUT_MS = 120_000L
    const val PARENT_FETCH_TIMEOUT_MS = 15_000L
    const val SNAPSHOT_SYNC_TIMESTAMP_THROTTLE_MS = 15_000L
    const val OUTBOX_BATCH_FETCH_SIZE = 300
    const val MAX_CONCURRENT_WRITES = 15
  }
}
