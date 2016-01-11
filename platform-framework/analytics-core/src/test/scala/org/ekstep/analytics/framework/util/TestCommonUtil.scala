package org.ekstep.analytics.framework.util

import org.ekstep.analytics.framework.BaseSpec
import org.joda.time.LocalDate
import java.io.File
import org.joda.time.DateTime
import java.util.Date
import java.text.SimpleDateFormat
import scala.collection.mutable.ListBuffer
import org.joda.time.format.DateTimeFormat
import org.ekstep.analytics.framework.JobConfig

class TestCommonUtil extends BaseSpec {

    it should "pass test case of all methods in CommonUtil" in {

        //checkContains
        CommonUtil.checkContains("IlimiIndia", "Ilimi") should be(true)

        //datesBetween
        val from = new LocalDate("2016-01-01");
        val to = new LocalDate("2016-01-04");
        CommonUtil.datesBetween(from, to).toArray should be(Array(new LocalDate("2016-01-01"), new LocalDate("2016-01-02"), new LocalDate("2016-01-03"), new LocalDate("2016-01-04")))

        //deleteDirectory
        val path = "/Users/amitBehera/TestDirectory";
        val dir = new File(path)
        val dirCreated = dir.mkdir;
        dirCreated should be(true);
        CommonUtil.deleteDirectory(path)
        dir.isDirectory() should be(false);

        //deleteFile
        val filePath = "/Users/amitBehera/test.txt";
        val file = new File(filePath);
        val created = file.createNewFile();
        created should be(true);
        CommonUtil.deleteFile(filePath)
        file.isFile() should be(false);

        //formatEventDate
        val date = new DateTime(1452492389000L)
        CommonUtil.formatEventDate(date) should be("2016-01-11T11:36:29+05:30")

        //getAge
        val dateformat = new SimpleDateFormat("dd/MM/yyyy");
        val dob = dateformat.parse("04/07/1990");
        CommonUtil.getAge(dob) should be(25)

        //getDatesBetween
        CommonUtil.getDatesBetween("2016-01-01", Option("2016-01-04")) should be(Array("2016-01-01", "2016-01-02", "2016-01-03", "2016-01-04"))

        //getEvent
        val line = "{\"eid\":\"OE_START\",\"ts\":\"2016-01-01T12:13:20+05:30\",\"@timestamp\":\"2016-01-02T00:59:22.917Z\",\"ver\":\"1.0\",\"gdata\":{\"id\":\"org.ekstep.aser.lite\",\"ver\":\"5.7\"},\"sid\":\"a6e4b3e2-5c40-4d5c-b2bd-44f1d5c7dd7f\",\"uid\":\"2ac2ebf4-89bb-4d5d-badd-ba402ee70182\",\"did\":\"828bd4d6c37c300473fb2c10c2d28868bb88fee6\",\"edata\":{\"eks\":{\"loc\":null,\"mc\":null,\"mmc\":null,\"pass\":null,\"qid\":null,\"qtype\":null,\"qlevel\":null,\"score\":0,\"maxscore\":0,\"res\":null,\"exres\":null,\"length\":null,\"exlength\":0.0,\"atmpts\":0,\"failedatmpts\":0,\"category\":null,\"current\":null,\"max\":null,\"type\":null,\"extype\":null,\"id\":null,\"gid\":null}}}";
        val event = CommonUtil.getEvent(line);
        event.uid should be("2ac2ebf4-89bb-4d5d-badd-ba402ee70182")

        //getEventDate yyyy-MM-dd'T'HH:mm:ssZZ
        val evDate = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ssZZ").parseLocalDate("2016-01-01T12:13:20+05:30").toDate;
        CommonUtil.getEventDate(event) should be(evDate)

        //getEventTs
        CommonUtil.getEventTS(event) should be(1451630600000L)

        //getGameId
        CommonUtil.getGameId(event) should be("org.ekstep.aser.lite")

        //getGameVersion
        CommonUtil.getGameVersion(event) should be("5.7")

        //getHourOfDay
        CommonUtil.getHourOfDay(1447154514000L, 1447158114000L) should be(ListBuffer(16, 17))

        //getInputPath(prefix,suffix)
        CommonUtil.getInputPath("local:///Users/amitBehera/github", "") should be("/Users/amitBehera/github")
        //getInputPaths(input)
        CommonUtil.getInputPaths("local:///Users,local:///Users/amitBehera") should be("/Users,/Users/amitBehera")
        //getInputPaths(prefix,suffix)
        CommonUtil.getInputPaths("local:///Users,local:///Users/amitBehera", null) should be("/Users,/Users/amitBehera")

        //getParallelization
        val config = new JobConfig(null, None, None, null, None, None, Option(10), Option("testApp"), Option(false));
        CommonUtil.getParallelization(config) should be(10)

        //getParallelization
        val con = Option(Map("search" -> null, "filters" -> null, "sort" -> null, "model" -> null, "modelParams" -> null, "output" -> null, "parallelization" -> "10", "appName" -> "testApp", "deviceMapping" -> null))
        CommonUtil.getParallelization(con) should be(10)

        //getPath
        CommonUtil.getPath(null, "/Users/amitBehera", "local") should be("/Users/amitBehera")

        //getStartDate
        CommonUtil.getStartDate(Option("2016-01-08"), 7) should be(Option("2016-01-01"))

        //getTimeDiff
        CommonUtil.getTimeDiff(1451650400000L, 1451650410000L) should be(Option(10d))

        //getTimeSpent
        CommonUtil.getTimeSpent("10") should be(Option(10d))

        //gzip
        val testPath = "/Users/amitBehera/test.txt";
        val f = new File(testPath)
        f.createNewFile()
        CommonUtil.gzip(testPath)
        new File("/Users/amitBehera/test.txt.gz").isFile() should be(true)
        CommonUtil.deleteFile(testPath);
        CommonUtil.deleteFile("/Users/amitBehera/test.txt.gz");

    }
}