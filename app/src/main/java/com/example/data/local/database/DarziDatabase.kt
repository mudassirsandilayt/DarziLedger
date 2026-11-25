package com.example.data.local.database

import android.content.Context
import androidx.room.Database
import androidx.room.Room
import androidx.room.RoomDatabase
import androidx.room.TypeConverters
import androidx.room.migration.Migration
import androidx.sqlite.db.SupportSQLiteDatabase
import com.example.data.local.daos.CustomerDao
import com.example.data.local.daos.DeviceSyncDao
import com.example.data.local.daos.MeasurementDao
import com.example.data.local.daos.OrderDao
import com.example.data.local.daos.OrderSyncLogDao
import com.example.data.local.daos.PaymentDao
import com.example.data.local.daos.ShopDao
import com.example.data.local.daos.SyncOutboxDao
import com.example.data.local.entities.CustomerEntity
import com.example.data.local.entities.DeviceConfigEntity
import com.example.data.local.entities.MeasurementEntity
import com.example.data.local.entities.OrderEntity
import com.example.data.local.entities.OrderNumberReservationEntity
import com.example.data.local.entities.OrderSyncLogEntity
import com.example.data.local.entities.PaymentEntity
import com.example.data.local.entities.PrinterConfigEntity
import com.example.data.local.entities.ShopEntity
import com.example.data.local.entities.SyncConflictEntity
import com.example.data.local.entities.SyncMetadataEntity
import com.example.data.local.entities.SyncOutboxEntity

@Database(
  entities = [
    ShopEntity::class,
    CustomerEntity::class,
    MeasurementEntity::class,
    OrderEntity::class,
    PaymentEntity::class,
    SyncOutboxEntity::class,
    SyncMetadataEntity::class,
    DeviceConfigEntity::class,
    OrderNumberReservationEntity::class,
    PrinterConfigEntity::class,
    SyncConflictEntity::class,
    OrderSyncLogEntity::class,
  ],
  version = 9,
  exportSchema = true,
)
@TypeConverters(DarziConverters::class)
abstract class DarziDatabase : RoomDatabase() {
  abstract fun shopDao(): ShopDao
  abstract fun customerDao(): CustomerDao
  abstract fun measurementDao(): MeasurementDao
  abstract fun orderDao(): OrderDao
  abstract fun paymentDao(): PaymentDao
  abstract fun syncOutboxDao(): SyncOutboxDao
  abstract fun deviceSyncDao(): DeviceSyncDao
  abstract fun orderSyncLogDao(): OrderSyncLogDao

  companion object {
    const val DATABASE_NAME = "darzi_ledger.db"

    val MIGRATION_1_2: Migration = object : Migration(1, 2) {
      override fun migrate(db: SupportSQLiteDatabase) {
        db.execSQL(
          """
          CREATE TABLE IF NOT EXISTS `sync_conflicts` (
            `conflictId` TEXT NOT NULL,
            `entityType` TEXT NOT NULL,
            `entityId` TEXT NOT NULL,
            `localPayloadJson` TEXT NOT NULL,
            `remotePayloadJson` TEXT NOT NULL,
            `localVersion` INTEGER NOT NULL,
            `remoteVersion` INTEGER NOT NULL,
            `detectedAt` INTEGER NOT NULL,
            `resolved` INTEGER NOT NULL,
            `resolvedAt` INTEGER,
            PRIMARY KEY(`conflictId`)
          )
          """.trimIndent(),
        )
        db.execSQL(
          "CREATE INDEX IF NOT EXISTS `index_sync_conflicts_resolved` ON `sync_conflicts` (`resolved`)"
        )
        db.execSQL(
          "CREATE INDEX IF NOT EXISTS `index_sync_conflicts_entityType_entityId` ON `sync_conflicts` (`entityType`, `entityId`)"
        )
      }
    }

    val MIGRATION_2_3: Migration = object : Migration(2, 3) {
      override fun migrate(db: SupportSQLiteDatabase) {
        db.execSQL(
          "ALTER TABLE `customers` ADD COLUMN `customId` TEXT NOT NULL DEFAULT ''"
        )
      }
    }

    val MIGRATION_3_4: Migration = object : Migration(3, 4) {
      override fun migrate(db: SupportSQLiteDatabase) {
        db.execSQL(
          "ALTER TABLE `device_config` ADD COLUMN `userUid` TEXT NOT NULL DEFAULT ''"
        )
        db.execSQL(
          "ALTER TABLE `device_config` ADD COLUMN `userRole` TEXT NOT NULL DEFAULT 'OWNER'"
        )
      }
    }

    val MIGRATION_4_5: Migration = object : Migration(4, 5) {
      override fun migrate(db: SupportSQLiteDatabase) {
        db.execSQL(
          """
          CREATE TABLE IF NOT EXISTS `measurements_new` (
            `measurementId` TEXT NOT NULL,
            `customerId` TEXT NOT NULL,
            `label` TEXT NOT NULL,
            `isCurrent` INTEGER NOT NULL,
            `kameezLambai` REAL NOT NULL,
            `tera` REAL NOT NULL,
            `bazu` REAL NOT NULL,
            `chati` REAL NOT NULL,
            `kamar` REAL NOT NULL,
            `gheraDaman` REAL NOT NULL,
            `galaCollar` REAL NOT NULL,
            `cuff` REAL NOT NULL,
            `shalwarLambai` REAL NOT NULL,
            `paincha` REAL NOT NULL,
            `gheraAsan` REAL NOT NULL,
            `neckStyle` TEXT,
            `damanStyle` TEXT,
            `pocketStyle` TEXT NOT NULL,
            `sleeveStyle` TEXT,
            `silaiStyle` TEXT,
            `notes` TEXT NOT NULL,
            `createdAt` INTEGER NOT NULL,
            `updatedAt` INTEGER NOT NULL,
            `version` INTEGER NOT NULL,
            `lastModifiedByDevice` TEXT NOT NULL,
            `isDeleted` INTEGER NOT NULL,
            PRIMARY KEY(`measurementId`),
            FOREIGN KEY(`customerId`) REFERENCES `customers`(`customerId`) ON UPDATE NO ACTION ON DELETE CASCADE
          )
          """.trimIndent(),
        )
        db.execSQL(
          """
          INSERT INTO `measurements_new` (
            `measurementId`, `customerId`, `label`, `isCurrent`,
            `kameezLambai`, `tera`, `bazu`, `chati`, `kamar`, `gheraDaman`,
            `galaCollar`, `cuff`, `shalwarLambai`, `paincha`, `gheraAsan`,
            `neckStyle`, `damanStyle`, `pocketStyle`, `sleeveStyle`, `silaiStyle`,
            `notes`, `createdAt`, `updatedAt`, `version`, `lastModifiedByDevice`, `isDeleted`
          )
          SELECT
            `measurementId`, `customerId`, `label`, `isCurrent`,
            `kameezLambai`, `tera`, `bazu`, `chati`, `kamar`, `gheraDaman`,
            `galaCollar`, `cuff`, `shalwarLambai`, `paincha`, `gheraAsan`,
            `neckStyle`, `damanStyle`, `pocketStyle`, `sleeveStyle`, `silaiStyle`,
            `notes`, `createdAt`, `updatedAt`, `version`, `lastModifiedByDevice`, `isDeleted`
          FROM `measurements`
          """.trimIndent(),
        )
        db.execSQL("DROP TABLE `measurements`")
        db.execSQL("ALTER TABLE `measurements_new` RENAME TO `measurements`")
        db.execSQL(
          "CREATE INDEX IF NOT EXISTS `index_measurements_customerId` ON `measurements` (`customerId`)",
        )
        db.execSQL(
          "CREATE INDEX IF NOT EXISTS `index_measurements_customerId_isCurrent` ON `measurements` (`customerId`, `isCurrent`)",
        )
      }
    }

    val MIGRATION_5_6: Migration = object : Migration(5, 6) {
      override fun migrate(db: SupportSQLiteDatabase) {
        db.execSQL(
          "ALTER TABLE `measurements` ADD COLUMN `shalwarGhair` REAL NOT NULL DEFAULT 0.0",
        )
        db.execSQL(
          "ALTER TABLE `measurements` ADD COLUMN `shalwarType` TEXT",
        )
        db.execSQL(
          "ALTER TABLE `measurements` ADD COLUMN `rawInputsJson` TEXT NOT NULL DEFAULT '{}'",
        )
      }
    }

    val MIGRATION_6_7: Migration = object : Migration(6, 7) {
      override fun migrate(db: SupportSQLiteDatabase) {
        db.execSQL(
          """
          CREATE TABLE IF NOT EXISTS `order_sync_logs` (
            `logId` TEXT NOT NULL,
            `orderId` TEXT NOT NULL,
            `orderNumber` INTEGER,
            `operation` TEXT NOT NULL,
            `status` TEXT NOT NULL,
            `summary` TEXT NOT NULL,
            `errorMessage` TEXT,
            `deviceId` TEXT NOT NULL,
            `orderVersion` INTEGER NOT NULL,
            `timestamp` INTEGER NOT NULL,
            PRIMARY KEY(`logId`)
          )
          """.trimIndent(),
        )
        db.execSQL(
          "CREATE INDEX IF NOT EXISTS `index_order_sync_logs_orderId` ON `order_sync_logs` (`orderId`)",
        )
        db.execSQL(
          "CREATE INDEX IF NOT EXISTS `index_order_sync_logs_timestamp` ON `order_sync_logs` (`timestamp`)",
        )
        db.execSQL(
          "CREATE INDEX IF NOT EXISTS `index_order_sync_logs_status` ON `order_sync_logs` (`status`)",
        )
      }
    }

    val MIGRATION_7_8: Migration = object : Migration(7, 8) {
      override fun migrate(db: SupportSQLiteDatabase) {
        db.execSQL(
          "ALTER TABLE `measurements` ADD COLUMN `shoulderType` TEXT",
        )
      }
    }

    val MIGRATION_8_9: Migration = object : Migration(8, 9) {
      override fun migrate(db: SupportSQLiteDatabase) {
        db.execSQL("ALTER TABLE `sync_metadata` ADD COLUMN `customersLastPulledAt` INTEGER NOT NULL DEFAULT 0")
        db.execSQL("ALTER TABLE `sync_metadata` ADD COLUMN `measurementsLastPulledAt` INTEGER NOT NULL DEFAULT 0")
        db.execSQL("ALTER TABLE `sync_metadata` ADD COLUMN `ordersLastPulledAt` INTEGER NOT NULL DEFAULT 0")
        db.execSQL("ALTER TABLE `sync_metadata` ADD COLUMN `paymentsLastPulledAt` INTEGER NOT NULL DEFAULT 0")
        db.execSQL("ALTER TABLE `sync_metadata` ADD COLUMN `reservationsLastPulledAt` INTEGER NOT NULL DEFAULT 0")
      }
    }

    @Volatile
    private var INSTANCE: DarziDatabase? = null

    fun getInstance(context: Context): DarziDatabase {
      return INSTANCE ?: synchronized(this) {
        INSTANCE ?: Room.databaseBuilder(
          context.applicationContext,
          DarziDatabase::class.java,
          DATABASE_NAME,
        )
          .addMigrations(
            MIGRATION_1_2,
            MIGRATION_2_3,
            MIGRATION_3_4,
            MIGRATION_4_5,
            MIGRATION_5_6,
            MIGRATION_6_7,
            MIGRATION_7_8,
            MIGRATION_8_9,
          )
          .build()
          .also { INSTANCE = it }
      }
    }
  }
}
