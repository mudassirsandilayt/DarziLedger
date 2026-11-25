package com.example.app

import android.content.Context
import com.example.core.model.DeviceRegistrationStatus
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import com.example.R
import com.example.core.preferences.DataStoreShopPreferencesRepository
import com.example.core.preferences.ShopPreferencesRepository
import com.example.data.backup.ShopBackupRepository
import com.example.data.local.database.DarziDatabase
import com.example.data.local.repository.CustomerRepository
import com.example.data.local.repository.DeviceSyncRepository
import com.example.data.local.repository.OrderRepository
import com.example.data.local.repository.ShopRepository
import com.example.data.remote.AuthSessionProvider
import com.example.data.remote.FirebaseAuthSessionProvider
import com.example.data.remote.ShopCloudRepository
import com.example.data.sync.AndroidConnectivityMonitor
import com.example.data.sync.ConnectivityMonitor
import com.example.data.sync.SyncEngine
import com.example.features.orders.OrderMediaStore
import com.example.features.printing.AndroidBluetoothPrinterClient
import com.example.features.printing.ThermalPrintManager
import com.example.features.printing.ThermalPrinterClient
import com.google.firebase.firestore.FirebaseFirestore

interface AppContainer {
  val database: DarziDatabase
  val shopPreferencesRepository: ShopPreferencesRepository
  val shopRepository: ShopRepository
  val customerRepository: CustomerRepository
  val orderRepository: OrderRepository
  val deviceSyncRepository: DeviceSyncRepository
  val shopBackupRepository: ShopBackupRepository
  val firestore: FirebaseFirestore
  val authSessionProvider: AuthSessionProvider
  val shopCloudRepository: ShopCloudRepository
  val connectivityMonitor: ConnectivityMonitor
  val syncEngine: SyncEngine
  val applicationScope: CoroutineScope
  val thermalPrinterClient: ThermalPrinterClient
  val thermalPrintManager: ThermalPrintManager
  val orderMediaStore: OrderMediaStore
}

class DefaultAppContainer(
  private val applicationContext: Context,
  authSessionProviderOverride: AuthSessionProvider? = null,
  firestoreOverride: FirebaseFirestore? = null,
  connectivityMonitorOverride: ConnectivityMonitor? = null,
  thermalPrinterClientOverride: ThermalPrinterClient? = null,
  shopPreferencesRepositoryOverride: ShopPreferencesRepository? = null,
  shopCloudRepositoryOverride: ShopCloudRepository? = null,
 ) : AppContainer {
  override val applicationScope: CoroutineScope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
  override val database: DarziDatabase by lazy {
    DarziDatabase.getInstance(applicationContext)
  }

  override val shopPreferencesRepository: ShopPreferencesRepository by lazy {
    shopPreferencesRepositoryOverride ?: DataStoreShopPreferencesRepository(applicationContext)
  }

  override val shopRepository: ShopRepository by lazy {
    ShopRepository(database)
  }

  override val customerRepository: CustomerRepository by lazy {
    CustomerRepository(database, shopRepository)
  }

  override val orderRepository: OrderRepository by lazy {
    OrderRepository(database, shopRepository)
  }

  override val deviceSyncRepository: DeviceSyncRepository by lazy {
    DeviceSyncRepository(database)
  }

  override val shopBackupRepository: ShopBackupRepository by lazy {
    ShopBackupRepository(
      database = database,
      shopRepository = shopRepository,
      shopPreferencesRepository = shopPreferencesRepository,
    )
  }

  override val firestore: FirebaseFirestore by lazy {
    firestoreOverride ?: FirebaseFirestore.getInstance(
      applicationContext.getString(R.string.firestore_database_id),
    )
  }

  override val authSessionProvider: AuthSessionProvider by lazy {
    authSessionProviderOverride ?: FirebaseAuthSessionProvider()
  }

  override val shopCloudRepository: ShopCloudRepository by lazy {
    shopCloudRepositoryOverride ?: ShopCloudRepository(
      db = firestore,
      authSessionProvider = authSessionProvider,
      shopRepository = shopRepository,
      shopPreferencesRepository = shopPreferencesRepository,
      deviceSyncRepository = deviceSyncRepository,
    )
  }

  override val connectivityMonitor: ConnectivityMonitor by lazy {
    connectivityMonitorOverride ?: AndroidConnectivityMonitor(applicationContext)
  }

  override val syncEngine: SyncEngine by lazy {
    SyncEngine(
      database = database,
      db = firestore,
      authSessionProvider = authSessionProvider,
      connectivityMonitor = connectivityMonitor,
      deviceSyncRepository = deviceSyncRepository,
      shopRepository = shopRepository,
      orderRepository = orderRepository,
      applicationScope = applicationScope,
    )
  }

  override val thermalPrinterClient: ThermalPrinterClient by lazy {
    thermalPrinterClientOverride ?: AndroidBluetoothPrinterClient(applicationContext)
  }

  override val thermalPrintManager: ThermalPrintManager by lazy {
    ThermalPrintManager(
      printerClient = thermalPrinterClient,
      deviceSyncRepository = deviceSyncRepository,
      shopRepository = shopRepository,
      shopPreferencesRepository = shopPreferencesRepository,
      customerRepository = customerRepository,
    )
  }

  override val orderMediaStore: OrderMediaStore by lazy {
    OrderMediaStore(applicationContext)
  }
}
