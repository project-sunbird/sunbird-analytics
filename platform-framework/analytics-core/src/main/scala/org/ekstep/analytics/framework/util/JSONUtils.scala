package org.ekstep.analytics.framework.util

import java.lang.reflect.ParameterizedType
import java.lang.reflect.Type

import org.json4s.DefaultFormats
import org.json4s.Extraction
import org.json4s.jackson.JsonMethods

import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

/**
 * @author Santhosh
 */
object JSONUtils {

    @transient val mapper = new ObjectMapper();
    mapper.registerModule(DefaultScalaModule);
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    @throws(classOf[Exception])
    def serialize(obj: AnyRef): String = {
        implicit val formats = DefaultFormats;
        JsonMethods.compact(Extraction.decompose(obj));
    }

    @throws(classOf[Exception])
    def deserialize[T: Manifest](value: String): T = mapper.readValue(value, typeReference[T]);

    private[this] def typeReference[T: Manifest] = new TypeReference[T] {
        override def getType = typeFromManifest(manifest[T])
    }

    
    private[this] def typeFromManifest(m: Manifest[_]): Type = {
        if (m.typeArguments.isEmpty) { m.runtimeClass }
        else new ParameterizedType {
            def getRawType = m.runtimeClass
            def getActualTypeArguments = m.typeArguments.map(typeFromManifest).toArray
            def getOwnerType = null
        }
    }
    
    /*
    private[this] def typeFromManifest(m: Manifest[_]): Type = {
        m.runtimeClass
    }*/
}