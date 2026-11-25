package com.example.data.sync

import com.example.core.model.DamanStyle
import com.example.core.model.MeasurementSnapshot
import com.example.core.model.NeckStyle
import com.example.core.model.OrderStatus
import com.example.core.model.PaymentMethod
import com.example.core.model.PaymentStatus
import com.example.core.model.PocketStyle
import com.example.core.model.PocketStyleCodec
import com.example.core.model.ShalwarType
import com.example.core.model.ShoulderType
import com.example.core.model.SilaiStyle
import com.example.core.model.SleeveStyle
import com.example.core.model.SnapshotJson
import com.example.core.model.StyleSnapshot
import com.example.data.local.entities.CustomerEntity
import com.example.data.local.entities.MeasurementEntity
import com.example.data.local.entities.OrderEntity
import com.example.data.local.entities.OrderNumberReservationEntity
import com.example.data.local.entities.PaymentEntity
import com.example.data.remote.CustomerDoc
import com.example.data.remote.MeasurementDoc
import com.example.data.remote.OrderDoc
import com.example.data.remote.OrderReservationDoc
import com.example.data.remote.PaymentDoc
import com.google.firebase.firestore.FieldValue

object SyncMappers {

  private inline fun <reified T : Enum<T>> safeEnum(raw: String?, default: T): T =
    enumValues<T>().firstOrNull { it.name == raw } ?: default

  private inline fun <reified T : Enum<T>> safeEnumOrNull(raw: String?): T? {
    val clean = raw?.trim().orEmpty()
    if (clean.isEmpty()) return null
    return enumValues<T>().firstOrNull { it.name == clean }
  }

  fun customerToCreateMap(uid: String, entity: CustomerEntity): Map<String, Any> {
    val map = mutableMapOf<String, Any>(
      "ownerUid" to uid,
      "name" to entity.name.trim().take(100),
      "phone" to entity.phone.trim().take(30),
      "normalizedPhone" to entity.normalizedPhone.trim().take(30),
      "notes" to entity.notes.trim().take(500),
      "customId" to entity.customId.trim().take(40),
      "version" to entity.version.coerceAtLeast(1L),
      "lastModifiedByDevice" to entity.lastModifiedByDevice,
      "deleted" to entity.isDeleted,
      "createdAtEpochMillis" to entity.createdAt.coerceAtLeast(0L),
      "updatedAtEpochMillis" to entity.updatedAt.coerceAtLeast(0L),
      "createdAt" to FieldValue.serverTimestamp(),
      "updatedAt" to FieldValue.serverTimestamp(),
    )
    entity.deletedAt?.let { map["deletedAtMillis"] = it.coerceAtLeast(0L) }
    return map
  }

  fun customerToUpdateMap(entity: CustomerEntity): Map<String, Any> {
    val map = mutableMapOf<String, Any>(
      "name" to entity.name.trim().take(100),
      "phone" to entity.phone.trim().take(30),
      "normalizedPhone" to entity.normalizedPhone.trim().take(30),
      "notes" to entity.notes.trim().take(500),
      "customId" to entity.customId.trim().take(40),
      "version" to entity.version.coerceAtLeast(1L),
      "lastModifiedByDevice" to entity.lastModifiedByDevice,
      "deleted" to entity.isDeleted,
      "updatedAtEpochMillis" to entity.updatedAt.coerceAtLeast(0L),
      "updatedAt" to FieldValue.serverTimestamp(),
    )
    entity.deletedAt?.let { map["deletedAtMillis"] = it.coerceAtLeast(0L) }
    return map
  }

  fun CustomerDoc.toLocalEntity(shopId: String): CustomerEntity = CustomerEntity(
    customerId = id,
    shopId = shopId,
    name = name,
    phone = phone,
    normalizedPhone = normalizedPhone,
    notes = notes,
    customId = customId,
    createdAt = createdAtEpochMillis.takeIf { it > 0L } ?: (createdAt?.toDate()?.time ?: System.currentTimeMillis()),
    updatedAt = updatedAtEpochMillis.takeIf { it > 0L } ?: (updatedAt?.toDate()?.time ?: System.currentTimeMillis()),
    version = version.coerceAtLeast(1L),
    lastModifiedByDevice = lastModifiedByDevice,
    isDeleted = deleted,
    deletedAt = deletedAtMillis,
  )

  fun measurementToCreateMap(uid: String, entity: MeasurementEntity): Map<String, Any> = mapOf(
    "ownerUid" to uid,
    "customerId" to entity.customerId,
    "label" to entity.label.trim().take(60).ifEmpty { "Standard" },
    "currentFlag" to entity.isCurrent,
    "kameezLambai" to entity.kameezLambai,
    "tera" to entity.tera,
    "bazu" to entity.bazu,
    "chati" to entity.chati,
    "kamar" to entity.kamar,
    "gheraDaman" to entity.gheraDaman,
    "galaCollar" to entity.galaCollar,
    "cuff" to entity.cuff,
    "shalwarLambai" to entity.shalwarLambai,
    "paincha" to entity.paincha,
    "gheraAsan" to entity.gheraAsan,
    "shalwarGhair" to entity.shalwarGhair,
    "neckStyle" to (entity.neckStyle?.name ?: ""),
    "damanStyle" to (entity.damanStyle?.name ?: ""),
    "pocketStyle" to PocketStyleCodec.encode(entity.pocketStyles),
    "sleeveStyle" to (entity.sleeveStyle?.name ?: ""),
    "silaiStyle" to (entity.silaiStyle?.name ?: ""),
    "shalwarType" to (entity.shalwarType?.name ?: ""),
    "shoulderType" to (entity.shoulderType?.name ?: ""),
    "rawInputsJson" to entity.rawInputsJson.ifBlank { "{}" }.take(500),
    "notes" to entity.notes.trim().take(500),
    "version" to entity.version.coerceAtLeast(1L),
    "lastModifiedByDevice" to entity.lastModifiedByDevice,
    "deleted" to entity.isDeleted,
    "createdAtEpochMillis" to entity.createdAt.coerceAtLeast(0L),
    "updatedAtEpochMillis" to entity.updatedAt.coerceAtLeast(0L),
    "createdAt" to FieldValue.serverTimestamp(),
    "updatedAt" to FieldValue.serverTimestamp(),
  )

  fun measurementToUpdateMap(entity: MeasurementEntity): Map<String, Any> = mapOf(
    "label" to entity.label.trim().take(60).ifEmpty { "Standard" },
    "currentFlag" to entity.isCurrent,
    "kameezLambai" to entity.kameezLambai,
    "tera" to entity.tera,
    "bazu" to entity.bazu,
    "chati" to entity.chati,
    "kamar" to entity.kamar,
    "gheraDaman" to entity.gheraDaman,
    "galaCollar" to entity.galaCollar,
    "cuff" to entity.cuff,
    "shalwarLambai" to entity.shalwarLambai,
    "paincha" to entity.paincha,
    "gheraAsan" to entity.gheraAsan,
    "shalwarGhair" to entity.shalwarGhair,
    "neckStyle" to (entity.neckStyle?.name ?: ""),
    "damanStyle" to (entity.damanStyle?.name ?: ""),
    "pocketStyle" to PocketStyleCodec.encode(entity.pocketStyles),
    "sleeveStyle" to (entity.sleeveStyle?.name ?: ""),
    "silaiStyle" to (entity.silaiStyle?.name ?: ""),
    "shalwarType" to (entity.shalwarType?.name ?: ""),
    "shoulderType" to (entity.shoulderType?.name ?: ""),
    "rawInputsJson" to entity.rawInputsJson.ifBlank { "{}" }.take(500),
    "notes" to entity.notes.trim().take(500),
    "version" to entity.version.coerceAtLeast(1L),
    "lastModifiedByDevice" to entity.lastModifiedByDevice,
    "deleted" to entity.isDeleted,
    "updatedAtEpochMillis" to entity.updatedAt.coerceAtLeast(0L),
    "updatedAt" to FieldValue.serverTimestamp(),
  )

  fun MeasurementDoc.toLocalEntity(): MeasurementEntity = MeasurementEntity(
    measurementId = id,
    customerId = customerId,
    label = label,
    isCurrent = currentFlag,
    kameezLambai = kameezLambai,
    tera = tera,
    bazu = bazu,
    chati = chati,
    kamar = kamar,
    gheraDaman = gheraDaman,
    galaCollar = galaCollar,
    cuff = cuff,
    shalwarLambai = shalwarLambai,
    paincha = paincha,
    gheraAsan = gheraAsan,
    shalwarGhair = shalwarGhair,
    neckStyle = safeEnumOrNull<NeckStyle>(neckStyle),
    damanStyle = safeEnumOrNull<DamanStyle>(damanStyle),
    pocketStyles = PocketStyleCodec.decode(pocketStyle),
    sleeveStyle = safeEnumOrNull<SleeveStyle>(sleeveStyle),
    silaiStyle = SilaiStyle.fromRaw(silaiStyle),
    shalwarType = safeEnumOrNull<ShalwarType>(shalwarType),
    shoulderType = safeEnumOrNull<ShoulderType>(shoulderType),
    rawInputsJson = rawInputsJson.ifBlank { "{}" },
    notes = notes,
    createdAt = createdAtEpochMillis.takeIf { it > 0L } ?: (createdAt?.toDate()?.time ?: System.currentTimeMillis()),
    updatedAt = updatedAtEpochMillis.takeIf { it > 0L } ?: (updatedAt?.toDate()?.time ?: System.currentTimeMillis()),
    version = version.coerceAtLeast(1L),
    lastModifiedByDevice = lastModifiedByDevice,
    isDeleted = deleted,
  )

  fun orderToCreateMap(uid: String, entity: OrderEntity): Map<String, Any> {
    val meas = SnapshotJson.decodeMeasurement(entity.measurementSnapshotJson)
    val style = SnapshotJson.decodeStyle(entity.styleSnapshotJson)
    val measMap = mutableMapOf<String, Any>(
      "kameezLambai" to meas.kameezLambai,
      "tera" to meas.tera,
      "bazu" to meas.bazu,
      "chati" to meas.chati,
      "kamar" to meas.kamar,
      "gheraDaman" to meas.gheraDaman,
      "galaCollar" to meas.galaCollar,
      "cuff" to meas.cuff,
      "shalwarLambai" to meas.shalwarLambai,
      "paincha" to meas.paincha,
      "gheraAsan" to meas.gheraAsan,
      "shalwarGhair" to meas.shalwarGhair,
      "rawInputsJson" to SnapshotJson.encodeRawMap(meas.rawValues).take(500),
      "notes" to meas.notes.trim().take(500),
    )
    meas.sourceMeasurementId?.takeIf { it.isNotBlank() }?.let {
      measMap["sourceMeasurementId"] = it.take(128)
    }

    val styleMap = mapOf(
      "neckStyle" to (style.neckStyle?.name ?: ""),
      "damanStyle" to (style.damanStyle?.name ?: ""),
      "pocketStyle" to PocketStyleCodec.encode(style.effectivePocketStyles()),
      "sleeveStyle" to (style.sleeveStyle?.name ?: ""),
      "silaiStyle" to (style.silaiStyle?.name ?: ""),
      "shalwarType" to (style.shalwarType?.name ?: ""),
      "shoulderType" to (style.shoulderType?.name ?: ""),
    )

    val map = mutableMapOf<String, Any>(
      "ownerUid" to uid,
      "customerId" to entity.customerId,
      "suitCount" to entity.suitCount.toLong().coerceIn(1L, 100L),
      "deliveryDateMillis" to entity.deliveryDate.coerceAtLeast(0L),
      "totalAmountPkr" to entity.totalAmountPkr.coerceAtLeast(0L),
      "paidAmountPkr" to entity.paidAmountPkr.coerceIn(0L, entity.totalAmountPkr.coerceAtLeast(0L)),
      "remainingAmountPkr" to (entity.totalAmountPkr - entity.paidAmountPkr).coerceAtLeast(0L),
      "orderStatus" to entity.orderStatus.name,
      "paymentStatus" to entity.paymentStatus.name,
      "measurementSnapshot" to measMap,
      "styleSnapshot" to styleMap,
      "notes" to entity.notes.trim().take(500),
      "version" to entity.version.coerceAtLeast(1L),
      "lastModifiedByDevice" to entity.lastModifiedByDevice,
      "deleted" to entity.isDeleted,
      "createdAtEpochMillis" to entity.createdAt.coerceAtLeast(0L),
      "updatedAtEpochMillis" to entity.updatedAt.coerceAtLeast(0L),
      "createdAt" to FieldValue.serverTimestamp(),
      "updatedAt" to FieldValue.serverTimestamp(),
    )
    entity.orderNumber?.takeIf { it >= 1001 }?.let { map["orderNumber"] = it.toLong() }
    return map
  }

  fun orderDetailsUpdateMap(
    entity: OrderEntity,
    includeOrderNumber: Boolean,
  ): Map<String, Any> {
    val map = mutableMapOf<String, Any>(
      "suitCount" to entity.suitCount.toLong().coerceIn(1L, 100L),
      "deliveryDateMillis" to entity.deliveryDate.coerceAtLeast(0L),
      "orderStatus" to entity.orderStatus.name,
      "notes" to entity.notes.trim().take(500),
      "version" to entity.version.coerceAtLeast(1L),
      "lastModifiedByDevice" to entity.lastModifiedByDevice,
      "deleted" to entity.isDeleted,
      "updatedAtEpochMillis" to entity.updatedAt.coerceAtLeast(0L),
      "updatedAt" to FieldValue.serverTimestamp(),
    )
    if (includeOrderNumber) {
      entity.orderNumber?.takeIf { it >= 1001 }?.let { map["orderNumber"] = it.toLong() }
    }
    return map
  }

  fun orderPaymentUpdateMap(entity: OrderEntity): Map<String, Any> = mapOf(
    "paidAmountPkr" to entity.paidAmountPkr.coerceIn(0L, entity.totalAmountPkr.coerceAtLeast(0L)),
    "remainingAmountPkr" to (entity.totalAmountPkr - entity.paidAmountPkr).coerceAtLeast(0L),
    "paymentStatus" to entity.paymentStatus.name,
    "version" to entity.version.coerceAtLeast(1L),
    "lastModifiedByDevice" to entity.lastModifiedByDevice,
    "updatedAtEpochMillis" to entity.updatedAt.coerceAtLeast(0L),
    "updatedAt" to FieldValue.serverTimestamp(),
  )

  fun OrderDoc.toLocalEntity(shopId: String): OrderEntity {
    val meas = MeasurementSnapshot(
      sourceMeasurementId = measurementSnapshot["sourceMeasurementId"] as? String,
      kameezLambai = (measurementSnapshot["kameezLambai"] as? Number)?.toDouble() ?: 38.0,
      tera = (measurementSnapshot["tera"] as? Number)?.toDouble() ?: 17.0,
      bazu = (measurementSnapshot["bazu"] as? Number)?.toDouble() ?: 23.0,
      chati = (measurementSnapshot["chati"] as? Number)?.toDouble() ?: 40.0,
      kamar = (measurementSnapshot["kamar"] as? Number)?.toDouble() ?: 0.0,
      gheraDaman = (measurementSnapshot["gheraDaman"] as? Number)?.toDouble() ?: 42.0,
      galaCollar = (measurementSnapshot["galaCollar"] as? Number)?.toDouble() ?: 15.5,
      cuff = (measurementSnapshot["cuff"] as? Number)?.toDouble() ?: 0.0,
      shalwarLambai = (measurementSnapshot["shalwarLambai"] as? Number)?.toDouble() ?: 39.0,
      paincha = (measurementSnapshot["paincha"] as? Number)?.toDouble() ?: 7.5,
      gheraAsan = (measurementSnapshot["gheraAsan"] as? Number)?.toDouble() ?: 0.0,
      shalwarGhair = (measurementSnapshot["shalwarGhair"] as? Number)?.toDouble() ?: 0.0,
      rawValues = SnapshotJson.decodeRawMap(measurementSnapshot["rawInputsJson"] as? String),
      notes = measurementSnapshot["notes"] as? String ?: "",
    )
    val decodedPockets = PocketStyleCodec.decode(styleSnapshot["pocketStyle"] as? String)
    val style = StyleSnapshot(
      neckStyle = safeEnumOrNull<NeckStyle>(styleSnapshot["neckStyle"] as? String),
      damanStyle = safeEnumOrNull<DamanStyle>(styleSnapshot["damanStyle"] as? String),
      pocketStyle = decodedPockets.firstOrNull(),
      pocketStyles = PocketStyleCodec.ordered(decodedPockets),
      sleeveStyle = safeEnumOrNull<SleeveStyle>(styleSnapshot["sleeveStyle"] as? String),
      silaiStyle = SilaiStyle.fromRaw(styleSnapshot["silaiStyle"] as? String),
      shalwarType = safeEnumOrNull<ShalwarType>(styleSnapshot["shalwarType"] as? String),
      shoulderType = safeEnumOrNull<ShoulderType>(styleSnapshot["shoulderType"] as? String),
    )
    return OrderEntity(
      orderId = id,
      shopId = shopId,
      customerId = customerId,
      orderNumber = orderNumber?.toInt(),
      suitCount = suitCount.toInt().coerceAtLeast(1),
      deliveryDate = deliveryDateMillis,
      totalAmountPkr = totalAmountPkr,
      paidAmountPkr = paidAmountPkr,
      remainingAmountPkr = remainingAmountPkr,
      orderStatus = safeEnum(orderStatus, OrderStatus.PENDING),
      paymentStatus = safeEnum(paymentStatus, PaymentStatus.UNPAID),
      measurementSnapshotJson = SnapshotJson.encodeMeasurement(meas),
      styleSnapshotJson = SnapshotJson.encodeStyle(style),
      notes = notes,
      createdAt = createdAtEpochMillis.takeIf { it > 0L } ?: (createdAt?.toDate()?.time ?: System.currentTimeMillis()),
      updatedAt = updatedAtEpochMillis.takeIf { it > 0L } ?: (updatedAt?.toDate()?.time ?: System.currentTimeMillis()),
      version = version.coerceAtLeast(1L),
      lastModifiedByDevice = lastModifiedByDevice,
      isDeleted = deleted,
    )
  }

  fun paymentToCreateMap(uid: String, entity: PaymentEntity): Map<String, Any> = mapOf(
    "ownerUid" to uid,
    "orderId" to entity.orderId,
    "amountPkr" to entity.amountPkr.coerceAtLeast(1L),
    "paymentMethod" to entity.paymentMethod.name,
    "note" to entity.note.trim().take(300),
    "deviceId" to entity.deviceId,
    "createdAtEpochMillis" to entity.createdAt.coerceAtLeast(0L),
    "updatedAtEpochMillis" to System.currentTimeMillis(),
    "createdAt" to FieldValue.serverTimestamp(),
    "updatedAt" to FieldValue.serverTimestamp(),
  )

  fun PaymentDoc.toLocalEntity(): PaymentEntity = PaymentEntity(
    paymentId = id,
    orderId = orderId,
    amountPkr = amountPkr,
    paymentMethod = safeEnum(paymentMethod, PaymentMethod.CASH),
    note = note,
    createdAt = createdAtEpochMillis.takeIf { it > 0L } ?: (createdAt?.toDate()?.time ?: System.currentTimeMillis()),
    deviceId = deviceId,
  )

  fun OrderReservationDoc.toLocalEntity(): OrderNumberReservationEntity = OrderNumberReservationEntity(
    reservationId = id,
    deviceId = deviceId,
    startNumber = startNumber.toInt(),
    endNumber = endNumber.toInt(),
    nextAvailableNumber = startNumber.toInt(),
    isExhausted = false,
    createdAt = createdAt?.toDate()?.time ?: System.currentTimeMillis(),
    updatedAt = updatedAt?.toDate()?.time ?: System.currentTimeMillis(),
  )
}
