package org.ekstep.ilimi.analytics.framework.conf

import java.util.Properties
import java.io.FileInputStream
import scala.io.Source

object AppConf {

    var initialized = false;
    var properties: Properties = null;

    def init() {
        if (!initialized) {
            val key = getConfigKey;
            Console.println("### Using configuration file - " + key + " ###");
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
        val sysenv = sys.env.getOrElse("config_env", null);
        val env = if(sysenv != null) sysenv else sys.props.getOrElse("env", "DEV");    
        "/" + env.toLowerCase() + ".config.properties";
    }

}