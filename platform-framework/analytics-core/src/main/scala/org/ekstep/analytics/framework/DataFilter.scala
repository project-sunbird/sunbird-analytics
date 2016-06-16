package org.ekstep.analytics.framework

import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.exception.DataFilterException
import scala.util.control.Breaks
import org.apache.commons.beanutils.BeanUtils
import org.apache.commons.beanutils.PropertyUtils
import org.ekstep.analytics.framework.filter.Matcher
import scala.collection.mutable.Buffer
import org.apache.log4j.Logger
import org.ekstep.analytics.framework.util.JobLogger

/**
 * @author Santhosh
 */
object DataFilter {

    /**
     * Execute multiple filters and sort
     */
    val className = "org.ekstep.analytics.framework.DataFilter"
    @throws(classOf[DataFilterException])
    def filterAndSort[T](events: RDD[T], filters: Option[Array[Filter]], sort: Option[Sort]): RDD[T] = {
        JobLogger.debug("Running the filter and sort process", className)
        val filteredEvents = if (filters.nonEmpty) { filter(events, filters.get) } else events;
        if (sort.nonEmpty) { sortBy(filteredEvents, sort.get) } else filteredEvents;
    }

    @throws(classOf[DataFilterException])
    def filter[T](events: RDD[T], filters: Array[Filter]): RDD[T] = {
        JobLogger.debug("Running the filter process", className)
        if (null != filters && filters.nonEmpty) {
            events.filter { event =>
                var valid = true;
                Breaks.breakable {
                    filters.foreach { filter =>
                        val value = getValue(event, filter.name);
                        valid = Matcher.compare(value, filter);
                        if (!valid) Breaks.break;
                    }
                }
                valid;
            }
        } else {
            events;
        }
    }

    @throws(classOf[DataFilterException])
    def filter[T](events: RDD[T], filter: Filter): RDD[T] = {
        if (null != filter) {
            events.filter { event =>
                val value = getValue(event, filter.name);
                Matcher.compare(value, filter);
            }
        } else {
            events;
        }
    }

    @throws(classOf[DataFilterException])
    def filter[T](events: Buffer[T], filter: Filter): Buffer[T] = {
        if (null != filter) {
            events.filter { event =>
                val value = getValue(event, filter.name);
                Matcher.compare(value, filter);
            }
        } else {
            events;
        }
    }

    def sortBy[T](events: RDD[T], sort: Sort): RDD[T] = {
        if (null != sort) {
            events.sortBy(f => getStringValue(f, sort.name), "asc".equalsIgnoreCase(sort.order.getOrElse("asc")), JobContext.parallelization);
        } else {
            events;
        }
    }

    private def getStringValue(event: Any, name: String): String = {
        val value = getValue(event, name);
        if (null == value) "" else value.toString()
    }

    private def getValue(event: Any, name: String): AnyRef = {
        name match {
            case "eventId" => getBeanProperty(event, "eid");
            case "ts"      => getBeanProperty(event, "ts");
            case "gameId" =>
                var gid = getBeanProperty(event, "edata.eks.gid");
                if (null == gid)
                    gid = getBeanProperty(event, "gdata.id");
                gid;
            case "gameVersion"      => getBeanProperty(event, "gdata.ver");
            case "userId"           => getBeanProperty(event, "uid");
            case "sessionId"        => getBeanProperty(event, "sid");
            case "telemetryVersion" => getBeanProperty(event, "ver");
            case "itemId"           => getBeanProperty(event, "edata.eks.qid");
            case _                  => getBeanProperty(event, name);
        }
    }

    private def getBeanProperty(event: Any, prop: String): AnyRef = {
        val obj = PropertyUtils.getProperty(event, prop);
        if (null != obj) {
            val objClass = obj.getClass.getName;
            objClass match {
                case "scala.Some" =>
                    obj.asInstanceOf[Some[AnyRef]].get;
                case "scala.None$" => null;
                case _             => obj.asInstanceOf[AnyRef];
            }
        } else {
            obj;
        }
    }

}