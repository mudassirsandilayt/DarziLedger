package com.example.data.remote

import androidx.annotation.Keep
import com.google.firebase.Timestamp
import com.google.firebase.firestore.DocumentId
import com.google.firebase.firestore.IgnoreExtraProperties
import com.google.firebase.firestore.ServerTimestamp

@Keep
@IgnoreExtraProperties
data class ShopDoc(
  @DocumentId val id: String = "",
  val ownerUid: String = "",
  val shopName: String = "Darzi Ledger",
  val contactNumber: String = "",
  val email: String = "",
  val nextOrderNumber: Long = 1001L,
  val version: Long = 1L,
  @ServerTimestamp val createdAt: Timestamp? = null,
  @ServerTimestamp val updatedAt: Timestamp? = null,
)

@Keep
@IgnoreExtraProperties
data class DeviceRegistrationDoc(
  @DocumentId val id: String = "",
  val ownerUid: String = "",
  val userUid: String = "",
  val userEmail: String = "",
  val userRole: String = "OWNER",
  val deviceName: String = "Shop Android Device",
  val deviceRole: String = "PHONE",
  val status: String = "AUTHORIZED",
  val appVersion: String = "1.0",
  @ServerTimestamp val registeredAt: Timestamp? = null,
  @ServerTimestamp val lastSeenAt: Timestamp? = null,
  @ServerTimestamp val updatedAt: Timestamp? = null,
)

@Keep
@IgnoreExtraProperties
data class ShopStaffUserDoc(
  @DocumentId val id: String = "",
  val ownerUid: String = "",
  val staffUid: String = "",
  val displayName: String = "",
  val email: String = "",
  val role: String = "STAFF",
  val status: String = "ACTIVE",
  @ServerTimestamp val createdAt: Timestamp? = null,
  @ServerTimestamp val updatedAt: Timestamp? = null,
)

@Keep
@IgnoreExtraProperties
data class StaffLinkDoc(
  @DocumentId val id: String = "",
  val staffUid: String = "",
  val ownerUid: String = "",
  val displayName: String = "",
  val email: String = "",
  val status: String = "ACTIVE",
  @ServerTimestamp val createdAt: Timestamp? = null,
  @ServerTimestamp val updatedAt: Timestamp? = null,
)

@Keep
@IgnoreExtraProperties
data class OrderReservationDoc(
  @DocumentId val id: String = "",
  val ownerUid: String = "",
  val deviceId: String = "",
  val startNumber: Long = 1001L,
  val endNumber: Long = 1050L,
  @ServerTimestamp val createdAt: Timestamp? = null,
  @ServerTimestamp val updatedAt: Timestamp? = null,
)

@Keep
@IgnoreExtraProperties
data class CustomerDoc(
  @DocumentId val id: String = "",
  val ownerUid: String = "",
  val name: String = "",
  val phone: String = "",
  val normalizedPhone: String = "",
  val notes: String = "",
  val customId: String = "",
  val version: Long = 1L,
  val lastModifiedByDevice: String = "",
  val deleted: Boolean = false,
  val deletedAtMillis: Long? = null,
  val createdAtEpochMillis: Long = 0L,
  val updatedAtEpochMillis: Long = 0L,
  @ServerTimestamp val createdAt: Timestamp? = null,
  @ServerTimestamp val updatedAt: Timestamp? = null,
)

@Keep
@IgnoreExtraProperties
data class MeasurementDoc(
  @DocumentId val id: String = "",
  val ownerUid: String = "",
  val customerId: String = "",
  val label: String = "Standard",
  val currentFlag: Boolean = true,
  val kameezLambai: Double = 0.0,
  val tera: Double = 0.0,
  val bazu: Double = 0.0,
  val chati: Double = 0.0,
  val kamar: Double = 0.0,
  val gheraDaman: Double = 0.0,
  val galaCollar: Double = 0.0,
  val cuff: Double = 0.0,
  val shalwarLambai: Double = 0.0,
  val paincha: Double = 0.0,
  val gheraAsan: Double = 0.0,
  val shalwarGhair: Double = 0.0,
  val neckStyle: String = "",
  val damanStyle: String = "",
  val pocketStyle: String = "",
  val sleeveStyle: String = "",
  val silaiStyle: String = "",
  val shalwarType: String = "",
  val shoulderType: String = "",
  val rawInputsJson: String = "{}",
  val notes: String = "",
  val version: Long = 1L,
  val lastModifiedByDevice: String = "",
  val deleted: Boolean = false,
  val createdAtEpochMillis: Long = 0L,
  val updatedAtEpochMillis: Long = 0L,
  @ServerTimestamp val createdAt: Timestamp? = null,
  @ServerTimestamp val updatedAt: Timestamp? = null,
)

@Keep
@IgnoreExtraProperties
data class OrderDoc(
  @DocumentId val id: String = "",
  val ownerUid: String = "",
  val customerId: String = "",
  val orderNumber: Long? = null,
  val suitCount: Long = 1L,
  val deliveryDateMillis: Long = 0L,
  val totalAmountPkr: Long = 0L,
  val paidAmountPkr: Long = 0L,
  val remainingAmountPkr: Long = 0L,
  val orderStatus: String = "PENDING",
  val paymentStatus: String = "UNPAID",
  val measurementSnapshot: Map<String, Any> = emptyMap(),
  val styleSnapshot: Map<String, Any> = emptyMap(),
  val notes: String = "",
  val version: Long = 1L,
  val lastModifiedByDevice: String = "",
  val deleted: Boolean = false,
  val createdAtEpochMillis: Long = 0L,
  val updatedAtEpochMillis: Long = 0L,
  @ServerTimestamp val createdAt: Timestamp? = null,
  @ServerTimestamp val updatedAt: Timestamp? = null,
)

@Keep
@IgnoreExtraProperties
data class PaymentDoc(
  @DocumentId val id: String = "",
  val ownerUid: String = "",
  val orderId: String = "",
  val amountPkr: Long = 0L,
  val paymentMethod: String = "CASH",
  val note: String = "",
  val deviceId: String = "",
  val createdAtEpochMillis: Long = 0L,
  val updatedAtEpochMillis: Long = 0L,
  @ServerTimestamp val createdAt: Timestamp? = null,
  @ServerTimestamp val updatedAt: Timestamp? = null,
)
