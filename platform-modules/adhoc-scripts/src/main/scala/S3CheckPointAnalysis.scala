import org.ekstep.analytics.framework.DataFetcher
import org.ekstep.analytics.framework.Fetcher
import org.ekstep.analytics.framework.Query
import org.ekstep.analytics.framework.V3Event
import org.ekstep.analytics.framework.util.CommonUtil

object S3CheckPointAnalysis extends optional.Application {

    def main(bucket: String, prefix: String, date: String) {

        val execTime = CommonUtil.time({
            val queries = Option(Array(Query(Option(bucket), Option(prefix), None, Option(date), Option(0))));
            implicit val sparkContext = CommonUtil.getSparkContext(10, "S3CheckPointAnalysis");
            val data = DataFetcher.fetchBatchData[V3Event](Fetcher("S3", None, queries)).repartition(20);

            Console.println("Total records:", data.count());
        })
        Console.println("Time taken to execute:", execTime._1);
    }

}