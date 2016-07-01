package org.ekstep.analytics.framework.conf

import java.util.Properties
import java.io.FileInputStream
import scala.io.Source
import org.ekstep.analytics.framework.util.JobLogger

object AppConf {

    var initialized = false;
    var properties: Properties = null;

    def init() {
        if (!initialized) {
            val key = getConfigKey;
            JobLogger.log("Config file used", AppConf.getClass.getName, None, Option(Map("file"->key)), None)
            val is = getClass.getResourceAsStream(key)
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
        val prop = sys.env.getOrElse(key, "");
        if (prop.nonEmpty) {
            prop;
        } else {
            properties.getProperty(key, "");
        }
    }

    def getAwsKey(): String = {
        getConfig("aws_key");
    }

    def getAwsSecret(): String = {
        getConfig("aws_secret");
    }
    
    def getConfigKey(): String = {
        val env = sys.props.getOrElse("env", "DEV");    
        "/" + env.toLowerCase() + ".config.properties";
    }

}