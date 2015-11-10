package org.ekstep.ilimi.analytics.framework.conf

import java.util.Properties
import java.io.FileInputStream
import scala.io.Source

object AppConf {

    var initialized = false;
    var properties: Properties = null;

    def init() {
        if (!initialized) {
            val is = getClass.getResourceAsStream("/config.properties")
            properties = new Properties();
            properties.load(is)
            is.close()
            initialized = true;
        }
    }

    def getConfig(key: String): String = {
        if (!initialized) {
            init();
        }
        if (sys.env.getOrElse(key, "").nonEmpty) {
            sys.env(key);
        } else {
            properties.getProperty(key);
        }
    }

    def getAwsKey(): String = {
        sys.env("aws.key");
    }

    def getAwsSecret(): String = {
        sys.env("aws.secret");
    }

}