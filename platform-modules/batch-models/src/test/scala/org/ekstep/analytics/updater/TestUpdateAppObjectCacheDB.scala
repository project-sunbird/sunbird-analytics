package org.ekstep.analytics.updater

import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.framework.ProfileEvent
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import org.ekstep.analytics.util.Constants
import org.ekstep.analytics.creation.model.CreationEvent

/**
 * @author Santhosh
 */
class TestUpdateAppObjectCacheDB extends SparkSpec(null) {
    
    override def beforeAll() {
        super.beforeAll()
        val connector = CassandraConnector(sc.getConf);
        val session = connector.openSession();
        session.execute("TRUNCATE " + Constants.CREATION_KEY_SPACE_NAME + "." + Constants.APP_OBJECT_CACHE_TABLE);
        session.execute("TRUNCATE " + Constants.CREATION_KEY_SPACE_NAME + "." + Constants.USER_PROFILE_TABLE);
    }
    
    "UpdateAppObjectCacheDB" should "update object cache in creation db" in {

        val rdd = loadFile[CreationEvent]("src/test/resources/object-lifecycle/test-data1.log");
        UpdateAppObjectCacheDB.execute(rdd, None);
        
        val object1 = sc.cassandraTable[AppObjectCache](Constants.CREATION_KEY_SPACE_NAME, Constants.APP_OBJECT_CACHE_TABLE).where("type = ? and id = ?", "User", "387").first();
        object1.`type` should be ("User");
        object1.id should be ("387");
        object1.subtype should be (Option(""));
        object1.code should be (Option(""));
        object1.name.get should be ("Test User");
        object1.parentid should be (Option(""));
        object1.state should be ("Create");
        object1.prevstate should be ("");
        
        val object2 = sc.cassandraTable[AppObjectCache](Constants.CREATION_KEY_SPACE_NAME, Constants.APP_OBJECT_CACHE_TABLE).where("type = ? and id = ?", "Partner", "org.ekstep.partner.31").first();
        object2.`type` should be ("Partner");
        object2.id should be ("org.ekstep.partner.31");
        object2.subtype should be (Option(""));
        object2.code should be (Option(""));
        object2.name.get should be ("Test Partner");
        object2.parentid should be (Option(""));
        object2.state should be ("Create");
        object2.prevstate should be ("");
        
        val profile = sc.cassandraTable[UserProfile](Constants.CREATION_KEY_SPACE_NAME, Constants.USER_PROFILE_TABLE).where("user_id = ?", "387").first();
        profile.access should be ("""[{"id":"2","value":"Registered"},{"id":"8","value":"Super Users"},{"id":"12","value":"Words-creator"},{"id":"13","value":"Quiz-items-creator"},{"id":"17","value":"Content-creator"},{"id":"19","value":"Concepts-creator"},{"id":"21","value":"Ekstep Admins"},{"id":"22","value":"Words-editor"},{"id":"23","value":"Partner-admin"}]""");
        profile.partners should be ("""[{"id":"org.ekstep.partner.pratham","value":"Pratham"},{"id":"org.ekstep.girlchild","value":"save girl child"}]""");
        profile.profile should be ("[]");
        profile.name.get should be ("Test User");
        profile.email.get should be ("test_user@ekstep.com");
        profile.user_id should be ("387");
    }
  
}