import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.Query
import org.ekstep.analytics.framework.DataFetcher
import org.ekstep.analytics.framework.Fetcher
import org.ekstep.analytics.framework.dispatcher.KafkaDispatcher
import org.ekstep.analytics.framework.OutputDispatcher
import org.ekstep.analytics.framework.Dispatcher
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.V3Event
import org.ekstep.analytics.framework.DataFilter
import org.ekstep.analytics.framework.Filter

object DikshaLoadAnalysis extends optional.Application {

    def main(bucket: String, prefix: String, date: String) {

        val execTime = CommonUtil.time({
            val queries = Option(Array(Query(Option(bucket), Option(prefix), None, Option(date), Option(0))));
            implicit val sparkContext = CommonUtil.getSparkContext(10, "DikshaLoadAnalysis");
            val events = DataFetcher.fetchBatchData[V3Event](Fetcher("S3", None, queries))
            val data = DataFilter.filter(events, Array(Filter("eid", "NE", Option("LOG")))).cache();

            val appEvents = data.filter(f => f.context.pdata.get.id.equals("prod.diksha.app")).cache;

            val contentDownloaded = DataFilter.filter(appEvents, Array(
                Filter("eid", "EQ", Option("SHARE")),
                Filter("edata.type", "EQ", Option("CONTENT")),
                Filter("edata.dir", "EQ", Option("In")))).cache();

            val contentDownloadedCount = contentDownloaded.count();
            val mostDownloadedContent = contentDownloaded.map(f => f.edata.items).flatMap(f => f.map(f => f)).groupBy(f => f.id).mapValues(f => f.size).sortBy(f => f._2, false, 1).collect().take(5);
            contentDownloaded.unpersist(true);

            val mostViewedContent = appEvents.filter(f => f.eid.equals("START") && f.context.pdata.get.pid.get.equals("sunbird.app.contentplayer")).map(f => f.`object`.get).groupBy(f => f.id).mapValues(f => f.size).sortBy(f => f._2, false, 1).collect().take(5);

            val distinctAppUsers = appEvents.map(f => f.actor.id).distinct().count();
            val distinctAppDevices = appEvents.map(f => f.context.did.get).distinct().count();
            val dialScans = appEvents.filter(f => f.eid.equals("START") && f.edata.`type`.equals("qr")).count;

            appEvents.unpersist(true);

            val portalEvents = data.filter(f => f.context.pdata.get.id.equals("prod.diksha.portal")).cache();
            val distinctPortalUsers = portalEvents.map(f => f.actor.id).distinct().count();
            val distinctPortalDevices = portalEvents.map(f => f.context.did.getOrElse("unknown")).distinct().count();
            val contentCreated = data.filter(f => f.eid.equals("AUDIT") && "Draft".equals(f.edata.state) && !f.context.channel.equals("in.ekstep")).count();
            val totalDialScans = data.filter(f => f.eid.equals("SEARCH")).map(f => (f.edata.size, f.edata.filters.get.asInstanceOf[Map[String, AnyRef]])).filter(f => f._2.contains("dialcodes")).cache();
            val totalDialScansCount = totalDialScans.count();
            val totalDialScansSuccess = totalDialScans.filter(f => f._1 > 0).count();

            val dialCodeLinked = data.filter(f => f.eid.equals("AUDIT") && f.edata.props != null && f.edata.props.contains("dialcodes")).count;

            Console.println("Total Contents Downloaded:", contentDownloadedCount);
            Console.println("Distinct App Users:", distinctAppUsers);
            Console.println("Distinct App Devices:", distinctAppDevices);
            Console.println("In APP Dial Scans:", dialScans);
            Console.println("Distinct Portal Users:", distinctPortalUsers);
            Console.println("Distinct Portal Devices:", distinctPortalDevices);
            Console.println("Content Created:", contentCreated);
            Console.println("Total Dial Scans:", totalDialScansCount);
            Console.println("Total Dial Scans Success:", totalDialScansSuccess);
            Console.println("Dial Codes Linked:", dialCodeLinked);

            Console.println("#### Content Most Downloaded ###");
            mostDownloadedContent.foreach(println(_))
            Console.println("#### Content Most Viewed ###");
            mostViewedContent.foreach(println(_))
        })
        Console.println("Time taken to execute:", execTime._1);
    }

}