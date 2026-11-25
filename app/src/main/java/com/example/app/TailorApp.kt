package com.example.app

import android.app.Application
import com.example.core.model.DeviceRegistrationStatus
import kotlinx.coroutines.flow.combine
import kotlinx.coroutines.flow.distinctUntilChanged
import kotlinx.coroutines.flow.collectLatest
import kotlinx.coroutines.launch

class TailorApp : Application() {
  lateinit var container: AppContainer

  override fun onCreate() {
    super.onCreate()
    container = DefaultAppContainer(this)

    // Keep Firestore listeners alive for the process lifetime, not tied to a screen/ViewModel.
    container.applicationScope.launch {
      combine(
        container.authSessionProvider.authStateFlow,
        container.deviceSyncRepository.observeDeviceConfig(),
      ) { user, device ->
        if (user == null || device?.registrationStatus != DeviceRegistrationStatus.AUTHORIZED) {
          null
        } else {
          device.shopId.takeIf { it.isNotBlank() && it != ShopRepository.DEFAULT_SHOP_ID } ?: user.uid
        }
      }
        .distinctUntilChanged()
        .collectLatest { shopId ->
          if (shopId == null) {
            container.syncEngine.stopRealtimeSync()
          } else {
            container.syncEngine.startRealtimeSync(shopId)
          }
        }
    }
  }
}
