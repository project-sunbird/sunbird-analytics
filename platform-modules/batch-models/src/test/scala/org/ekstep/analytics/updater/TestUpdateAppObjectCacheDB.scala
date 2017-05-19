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
    
    "UpdateAppObjectCacheDB" should "create app object cache in creation db" in {

        val rdd = loadFile[CreationEvent]("src/test/resources/object-lifecycle/test-data1.log");
        UpdateAppObjectCacheDB.execute(rdd, None);
        
        val object1 = sc.cassandraTable[AppObjectCache](Constants.CREATION_KEY_SPACE_NAME, Constants.APP_OBJECT_CACHE_TABLE).where("type = ? and id = ?", "User", "387").first();
        object1.`type` should be ("User");
        object1.id should be ("387");
        object1.subtype should be ("");
        object1.code should be (Option(""));
        object1.name should be ("Test User");
        object1.parentid should be (Option(""));
        object1.state should be ("Create");
        object1.prevstate should be ("");
        
        val object2 = sc.cassandraTable[AppObjectCache](Constants.CREATION_KEY_SPACE_NAME, Constants.APP_OBJECT_CACHE_TABLE).where("type = ? and id = ?", "Partner", "org.ekstep.partner.31").first();
        object2.`type` should be ("Partner");
        object2.id should be ("org.ekstep.partner.31");
        object2.subtype should be ("");
        object2.code should be (Option(""));
        object2.name should be ("Test Partner");
        object2.parentid should be (Option(""));
        object2.state should be ("Create");
        object2.prevstate should be ("");
        
        val profile = sc.cassandraTable[UserProfile](Constants.CREATION_KEY_SPACE_NAME, Constants.USER_PROFILE_TABLE).where("user_id = ?", "387").first();
        profile.access should be ("""[{"id":"2","value":"Registered"},{"id":"8","value":"Super Users"},{"id":"12","value":"Words-creator"},{"id":"13","value":"Quiz-items-creator"},{"id":"17","value":"Content-creator"},{"id":"19","value":"Concepts-creator"},{"id":"21","value":"Ekstep Admins"},{"id":"22","value":"Words-editor"},{"id":"23","value":"Partner-admin"}]""");
        profile.partners should be ("""[{"id":"org.ekstep.partner.pratham","value":"Pratham"},{"id":"org.ekstep.girlchild","value":"save girl child"}]""");
        profile.profile should be ("[]");
        profile.name should be ("Test User");
        profile.email.get should be ("test_user@ekstep.com");
        profile.user_id should be ("387");
    }
    
    it should "update app object cache in creation db" in {

        val rdd = loadFile[CreationEvent]("src/test/resources/object-lifecycle/test-data2.log");
        UpdateAppObjectCacheDB.execute(rdd, None);
        
        val object1 = sc.cassandraTable[AppObjectCache](Constants.CREATION_KEY_SPACE_NAME, Constants.APP_OBJECT_CACHE_TABLE).where("type = ? and id = ?", "User", "387").first();
        object1.`type` should be ("User");
        object1.id should be ("387");
        object1.subtype should be ("");
        object1.code should be (Option(""));
        object1.name should be ("Test User");
        object1.parentid should be (Option(""));
        object1.state should be ("Delete");
        object1.prevstate should be ("Create");
        
        val object2 = sc.cassandraTable[AppObjectCache](Constants.CREATION_KEY_SPACE_NAME, Constants.APP_OBJECT_CACHE_TABLE).where("type = ? and id = ?", "Partner", "org.ekstep.partner.31").first();
        object2.`type` should be ("Partner");
        object2.id should be ("org.ekstep.partner.31");
        object2.subtype should be ("");
        object2.code should be (Option(""));
        object2.name should be ("Test Partner2");
        object2.parentid should be (Option(""));
        object2.state should be ("Update");
        object2.prevstate should be ("Create");
        
        val object3 = sc.cassandraTable[AppObjectCache](Constants.CREATION_KEY_SPACE_NAME, Constants.APP_OBJECT_CACHE_TABLE).where("type = ? and id = ?", "Tag", "53fcae3d37197b1e1effc36eaeb523426ac5b0c0").first();
        object3.`type` should be ("Tag");
        object3.id should be ("53fcae3d37197b1e1effc36eaeb523426ac5b0c0");
        object3.subtype should be ("");
        object3.code should be (Option(""));
        object3.name should be ("Test Tag2");
        object3.parentid should be (Option("org.ekstep.partner.31"));
        object3.state should be ("Update");
        object3.prevstate should be ("Create");
        
        val profile = sc.cassandraTable[UserProfile](Constants.CREATION_KEY_SPACE_NAME, Constants.USER_PROFILE_TABLE).where("user_id = ?", "387").first();
        profile.access should be ("""[{"id":"2","value":"Registered"},{"id":"17","value":"Content-creator"}]""");
        profile.partners should be ("""[{"id":"org.ekstep.girlchild","value":"save girl child"}]""");
        profile.profile should be ("[]");
        profile.name should be ("Test User");
        profile.email.get should be ("test_user@ekstep.com");
        profile.user_id should be ("387");
    }
    
    it should "create platform object cache in creation db" in {
        val rdd = loadFile[CreationEvent]("src/test/resources/object-lifecycle/test-data3.log");
        UpdateAppObjectCacheDB.execute(rdd, None);
        
        val object1 = sc.cassandraTable[AppObjectCache](Constants.CREATION_KEY_SPACE_NAME, Constants.APP_OBJECT_CACHE_TABLE).where("type = ? and id = ?", "Content", "do_11224788307740262413").first();
        object1.`type` should be ("Content");
        object1.id should be ("do_11224788307740262413");
        object1.subtype should be ("TextBookUnit");
        object1.code should be (Option("org.sunbird.UarOnQ"));
        object1.name should be ("Unit 1.1");
        object1.parentid should be (Option("do_11224788307740262413"));
        object1.parenttype should be (Option("Content"));
        object1.state should be ("Draft");
        object1.prevstate should be ("");
        
        val object2 = sc.cassandraTable[AppObjectCache](Constants.CREATION_KEY_SPACE_NAME, Constants.APP_OBJECT_CACHE_TABLE).where("type = ? and id = ?", "Content", "do_11224796303869542413").first();
        object2.`type` should be ("Content");
        object2.id should be ("do_11224796303869542413");
        object2.subtype should be ("Story");
        object2.code should be (Option("org.ekstep.literacy.story.4360"));
        object2.name should be ("testContent");
        object2.parentid should be (Option(""));
        object2.parenttype should be (None);
        object2.state should be ("Processing");
        object2.prevstate should be ("Draft");
        
    }
  
}