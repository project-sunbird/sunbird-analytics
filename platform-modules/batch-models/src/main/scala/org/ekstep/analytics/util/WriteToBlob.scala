package org.ekstep.analytics.util

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext

object WriteToBlob {
  def renameHadoopFiles(tempDir: String, outDir: String, id: String, dims: List[String])(implicit sc: SparkContext): String = {
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val fileList = fs.listFiles(new Path(s"$tempDir/"), true)

    while (fileList.hasNext) {
      val filePath = fileList.next().getPath.toString
      if (!filePath.contains("_SUCCESS")) {
        val breakUps = filePath.split("/").filter(f => {
          f.contains("=")})
        val dimsKeys = breakUps.filter { f =>
          val bool = dims.map(x => f.contains(x))
          if (bool.contains(true)) true
          else false
        }
        val finalKeys = dimsKeys.map { f =>
          f.split("=").last
        }
        val key = if (finalKeys.isEmpty) {
          id
        } else if (finalKeys.length > 1) id + "/" + finalKeys.mkString("/")
        else id + "/" + finalKeys.head

        val crcKey = if (finalKeys.isEmpty) {
          if (id.contains("/")) {
            val ids = id.split("/")
            ids.head + "/." + ids.last
          } else "." + id
        } else if (finalKeys.length > 1) {
          val builder = new StringBuilder
          val keyStr = finalKeys.mkString("/")
          val replaceStr = "/."
          builder.append(id + "/")
          builder.append(keyStr.substring(0, keyStr.lastIndexOf("/")))
          builder.append(replaceStr)
          builder.append(keyStr.substring(keyStr.lastIndexOf("/") + replaceStr.length - 1))
          builder.mkString
        } else
          id + "/." + finalKeys.head
        val finalCSVPath = s"$outDir$key.csv"
        val finalCRCPath = s"$outDir$crcKey.csv.crc"
        fs.rename(new Path(filePath), new Path(finalCSVPath))
        fs.delete(new Path(finalCRCPath), false)
      }

    }
    outDir
  }
}