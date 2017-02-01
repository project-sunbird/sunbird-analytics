package org.ekstep.analytics.framework.conf

import java.util.Properties
import java.io.FileInputStream
import scala.io.Source
import org.ekstep.analytics.framework.util.JobLogger
import com.typesafe.config.ConfigFactory
import com.typesafe.config.ConfigParseOptions
import com.typesafe.config.ConfigResolveOptions

object AppConf {

    implicit val className = "org.ekstep.analytics.framework.conf.AppConf";

    lazy val conf = ConfigFactory.load();
    lazy val env = ConfigFactory.systemEnvironment();

    def getConfig(key: String): String = {
        if (env.hasPath(key))
            env.getString(key);
        else if (conf.hasPath(key))
            conf.getString(key);
        else "";
    }

    def getAwsKey(): String = {
        getConfig("aws_key");
    }

    def getAwsSecret(): String = {
        getConfig("aws_secret");
    }

}