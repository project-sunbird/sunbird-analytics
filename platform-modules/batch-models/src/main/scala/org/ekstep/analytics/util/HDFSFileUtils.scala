package org.ekstep.analytics.util

import java.io.File
import java.nio.file.{Files, StandardCopyOption}

import org.ekstep.analytics.framework.util.JobLogger

class HDFSFileUtils(classNameStr: String, jobLogger: JobLogger.type ) {
  implicit val className = classNameStr

  private def recursiveListFiles(file: File, ext: String): Array[File] = {
    val fileList = file.listFiles
    val extOnly = fileList.filter(file => file.getName.endsWith(ext))
    extOnly ++ fileList.filter(_.isDirectory).flatMap(recursiveListFiles(_, ext))
  }

  def purgeDirectory(dir: File): Unit = {
    for (file <- dir.listFiles) {
      if (file.isDirectory) purgeDirectory(file)
      file.delete
    }
  }

  def purgeDirectory(dirName: String): Unit = {
    val dir = new File(dirName)
    purgeDirectory(dir)
  }

  def renameReport(tempDir: String, outDir: String, fileExt: String, fileNameSuffix: String = null) = {
    val regex = """\=.*/""".r // example path "somepath/partitionFieldName=12313144/part-0000.csv"
    val temp = new File(tempDir)
    val out = new File(outDir)

    if (!temp.exists()) throw new Exception(s"path $tempDir doesn't exist")

    if (!out.exists()) {
      out.mkdirs()
      jobLogger.log(s"creating the directory ${out.getPath}")
    }

    val fileList = recursiveListFiles(temp, fileExt)

    jobLogger.log(s"moving ${fileList.length} files to ${out.getPath}")

    fileList.foreach(file => {
      val value = regex.findFirstIn(file.getPath).getOrElse("")
      if (value.length > 1) {
        val partitionFieldName = value.substring(1, value.length() - 1)
        var filePath = s"${out.getPath}/$partitionFieldName-$fileNameSuffix$fileExt"

        Files.copy(file.toPath, new File(s"${filePath}").toPath(), StandardCopyOption.REPLACE_EXISTING)
        jobLogger.log(s"${partitionFieldName} Copied from ${file.toPath.toAbsolutePath()} to ${filePath}" )

      }
    })
  }

}
