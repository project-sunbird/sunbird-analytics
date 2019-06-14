package org.ekstep.analytics.updater

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import org.ekstep.analytics.framework.DerivedEvent
import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.util.Constants

class TestUpdateDeviceProfileDB extends SparkSpec(null) {
    
    "UpdateDeviceProfileDB" should "create device profile in device db" in {
        CassandraConnector(sc.getConf).withSessionDo { session =>
            session.execute("TRUNCATE " + Constants.DEVICE_KEY_SPACE_NAME + "." + Constants.DEVICE_PROFILE_TABLE);
        }
        val rdd = loadFile[DerivedEvent]("src/test/resources/device-profile/test-data1.log");
        UpdateDeviceProfileDB.execute(rdd, None);

        val device1 = sc.cassandraTable[DeviceProfileOutput](Constants.DEVICE_KEY_SPACE_NAME, Constants.DEVICE_PROFILE_TABLE).where("device_id=?", "88edda82418a1e916e9906a2fd7942cb").first
        device1.first_access.get should be(1537550355883L)
        device1.last_access.get should be(1537625381139L)
        device1.total_ts.get should be(50)
        device1.total_launches.get should be(1)
        device1.avg_ts.get should be(50)

        val device2 = sc.cassandraTable[DeviceProfileOutput](Constants.DEVICE_KEY_SPACE_NAME, Constants.DEVICE_PROFILE_TABLE).where("device_id=?", "48edda82418a1e916e9906a2fd7942cb").first
        device2.first_access.get should be(1537550355883L)
        device2.last_access.get should be(1537550364377L)
        device2.total_ts.get should be(18)
        device2.total_launches.get should be(2)
        device2.avg_ts.get should be(9)
    }
    
    it should "check for first_access and last_access" in {
        CassandraConnector(sc.getConf).withSessionDo { session =>
            session.execute("TRUNCATE " + Constants.DEVICE_KEY_SPACE_NAME + "." + Constants.DEVICE_PROFILE_TABLE);
            session.execute("INSERT INTO " + Constants.DEVICE_KEY_SPACE_NAME + "." + Constants.DEVICE_PROFILE_TABLE +"(device_id, first_access, last_access, total_ts, total_launches, avg_ts, state, city, device_spec, uaspec, updated_date) VALUES ('48edda82418a1e916e9906a2fd7942cb', 1537550355883, 1537550364377, 18, 2, 9, 'Karnataka', 'Bangalore', {'os':'Android 6.0', 'make':'Motorola XT1706'}, {'raw':'xyz'}, 0);");
            session.execute("INSERT INTO " + Constants.DEVICE_KEY_SPACE_NAME + "." + Constants.DEVICE_PROFILE_TABLE +"(device_id, first_access, last_access, total_ts, total_launches, avg_ts, updated_date) VALUES ('88edda82418a1e916e9906a2fd7942cb', 1537550355883, 1537550364377, 20, 2, 10, 0);");
        }
        val rdd = loadFile[DerivedEvent]("src/test/resources/device-profile/test-data2.log");
        UpdateDeviceProfileDB.execute(rdd, None);

        val device1 = sc.cassandraTable[DeviceProfileOutput](Constants.DEVICE_KEY_SPACE_NAME, Constants.DEVICE_PROFILE_TABLE).where("device_id=?", "48edda82418a1e916e9906a2fd7942cb").first
        device1.first_access.get should be(1537550355883L)
        device1.last_access.get should be(1537660464377L)
        device1.total_ts.get should be(28)
        device1.total_launches.get should be(3)
        device1.avg_ts.get should be(9.33)
        device1.city.get should be("Bangalore")
        device1.state.get should be("Karnataka")
        device1.device_spec.get should be(Map("os" -> "Android 6.0", "make" -> "Motorola XT1706"))
        device1.uaspec.get should be(Map("raw" -> "xyz"))

        val device2 = sc.cassandraTable[DeviceProfileOutput](Constants.DEVICE_KEY_SPACE_NAME, Constants.DEVICE_PROFILE_TABLE).where("device_id=?", "88edda82418a1e916e9906a2fd7942cb").first
        device2.first_access.get should be(1537450355883L)
        device2.last_access.get should be(1537550364377L)
        device2.total_ts.get should be(45)
        device2.total_launches.get should be(3)
        device2.avg_ts.get should be(15)
    }

    it should "Handle null values from Cassandra and execute successfully" in {
        CassandraConnector(sc.getConf).withSessionDo { session =>
            session.execute("TRUNCATE " + Constants.DEVICE_KEY_SPACE_NAME + "." + Constants.DEVICE_PROFILE_TABLE)
            session.execute("INSERT INTO " + Constants.DEVICE_KEY_SPACE_NAME + "." + Constants.DEVICE_PROFILE_TABLE +"(device_id,first_access, last_access, total_ts, total_launches, avg_ts, state, city, device_spec, uaspec, updated_date) VALUES ('48edda82418a1e916e9906a2fd7942cb',1537550355883, 1537550364377, 18, 2, 9, 'Karnataka', 'Bangalore', {'os':'Android 6.0', 'make':'Motorola XT1706'}, {'raw':'xyz'}, 0)")
            session.execute("INSERT INTO " + Constants.DEVICE_KEY_SPACE_NAME + "." + Constants.DEVICE_PROFILE_TABLE +"(device_id) VALUES ('88edda82418a1e916e9906a2fd7942cb')")
        }
        val rdd = loadFile[DerivedEvent]("src/test/resources/device-profile/test-data2.log")
        UpdateDeviceProfileDB.execute(rdd, None)
    }

    it should "include new values and execute successfully" in {
        CassandraConnector(sc.getConf).withSessionDo { session =>
            session.execute("TRUNCATE " + Constants.DEVICE_KEY_SPACE_NAME + "." + Constants.DEVICE_PROFILE_TABLE)
            session.execute("INSERT INTO " + Constants.DEVICE_KEY_SPACE_NAME + "." + Constants.DEVICE_PROFILE_TABLE +"(device_id,  state_custom, state_code_custom, district_custom, fcm_token, producer_id) VALUES ('88edda82418a1e916e9906a2fd7942cb', 'karnataka', '29', 'bangalore', 'token-xyz', 'sunbird-app')")
            session.execute("INSERT INTO " + Constants.DEVICE_KEY_SPACE_NAME + "." + Constants.DEVICE_PROFILE_TABLE +"(device_id,  state_custom, state_code_custom, district_custom, fcm_token, producer_id) VALUES ('test-device-1', 'Karnataka', '29', 'Bangalore', '', 'sunbird-portal')")
        }
        val rdd = loadFile[DerivedEvent]("src/test/resources/device-profile/test-data2.log")
        UpdateDeviceProfileDB.execute(rdd, None)
        val device = sc.cassandraTable[DeviceProfileOutput](Constants.DEVICE_KEY_SPACE_NAME, Constants.DEVICE_PROFILE_TABLE).where("device_id=?", "88edda82418a1e916e9906a2fd7942cb").first
        device.state_custom.get should be("karnataka")
        device.state_code_custom.get should be("29")
        device.district_custom.get should be("bangalore")
        device.fcm_token.get should be("token-xyz")
        device.producer_id.get should be("sunbird-app")
        val device2 = sc.cassandraTable[DeviceProfileOutput](Constants.DEVICE_KEY_SPACE_NAME, Constants.DEVICE_PROFILE_TABLE).where("device_id=?", "48edda82418a1e916e9906a2fd7942cb").first
        device2.state_custom should be(None)
        device2.state_code_custom should be(None)
        device2.district_custom should be(None)
        device2.fcm_token should be(None)
        device2.producer_id should be(None)
        val device3 = sc.cassandraTable[DeviceProfileOutput](Constants.DEVICE_KEY_SPACE_NAME, Constants.DEVICE_PROFILE_TABLE).where("device_id=?", "test-device-1").first
        device3.fcm_token.get should be("")
        device3.producer_id.get should be("sunbird-portal")
    }
}