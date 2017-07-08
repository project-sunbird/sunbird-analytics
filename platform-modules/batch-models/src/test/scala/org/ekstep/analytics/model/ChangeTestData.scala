package org.ekstep.analytics.model


import org.ekstep.analytics.framework._

case class UserProfile(uid: String, gender: String, age: Int);
case class DerivedEventTest(eid: String, ets: Long, syncts: Long, ver: String, mid: String, uid: String, channel: Option[String], content_id: Option[String] = None, cdata: Option[CData], context: Context, dimensions: Dimensions, edata: MEEdata, tags: Option[AnyRef] = None) extends Input with AlgoInput;

class ChangeTestData extends SparkSpec(null) {

    "ChangeTestData" should " convert data successfully" in {
        def _getETags(event: DerivedEventTest): ETags = {

            if (event.tags.isDefined) {
                val tags = event.tags.get.asInstanceOf[List[Map[String, List[String]]]]
                val genieTags = tags.filter(f => f.contains("genie")).map { x => x.get("genie").get }.flatMap { x => x }
                val partnerTags = tags.filter(f => f.contains("partnerid")).map { x => x.get("partnerid").get }.flatMap { x => x }
                val dims = tags.filter(f => f.contains("dims")).map { x => x.get("dims").get }.flatMap { x => x }
                ETags(Option(genieTags), Option(partnerTags), Option(dims))
            } else ETags(None, None, None)
        }

        val files = Array("item_summary_1.log", "item_summary_2.log", "item_summary_3.log")
        for(file <- files){
            val rdd = loadFile[DerivedEventTest]("src/test/resources/item-summary-model/"+file);

        println("data loaded successfully....")

        val data = rdd.map { x =>
            val etags = _getETags(x)
             DerivedEvent(x.eid, x.ets, x.syncts, x.ver, x.mid, x.uid, x.channel, x.content_id, x.cdata, x.context, x.dimensions, x.edata, Option(etags))
        }

        val path = "/Users/amitBehera/backup/data/"+file
        println("Path created successfully....")
        OutputDispatcher.dispatch(Dispatcher("file", Map("file" -> path)), data);
        }
        
    }
}