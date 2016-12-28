package org.ekstep.analytics.framework.util

import org.jets3t.service.security.AWSCredentials
import org.ekstep.analytics.framework.conf.AppConf
import org.jets3t.service.impl.rest.httpclient.RestS3Service
import org.jets3t.service.model.S3Object
import java.nio.file.Files
import java.io.File
import org.jets3t.service.acl.AccessControlList
import org.jets3t.service.acl.GroupGrantee
import org.jets3t.service.acl.Permission
import org.jets3t.service.acl.GranteeInterface
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Buffer
import java.nio.file.Paths
import org.jets3t.service.S3ServiceException
import org.ekstep.analytics.framework.Level._
import java.io.InputStream

object S3Util {

    implicit val className = "org.ekstep.analytics.framework.util.S3Util"

    private val awsCredentials = new AWSCredentials(AppConf.getAwsKey(), AppConf.getAwsSecret());
    private val s3Service = new RestS3Service(awsCredentials);

    def upload(bucketName: String, filePath: String, key: String) {
        JobLogger.log("Uploading file to S3. Bucket", Option(Map("bucketName" -> bucketName, "FilePath" -> filePath)))
        val s3Object = new S3Object(new File(filePath));
        s3Object.setKey(key)
        val fileObj = s3Service.putObject(bucketName, s3Object);
        JobLogger.log("File upload successful", Option(Map("etag" -> fileObj.getETag)))
    }

    def uploadDirectory(bucketName: String, prefix: String, dir: String) {

        val d = new File(dir)
        val files = if (d.exists && d.isDirectory) {
            d.listFiles.filter(_.isFile).toList;
        } else {
            List[File]();
        }
        for (f <- files) {
            val key = prefix + f.getName.split("/").last
            upload(bucketName, f.getAbsolutePath, key)
        }
    }

    def download(bucketName: String, prefix: String, localPath: String) {

        val objectArr = s3Service.listObjects(bucketName, prefix, null)
        val objects = getAllKeys(bucketName, prefix)
        for (obj <- objectArr) {
            val key = obj.getKey
            val file = key.split("/").last
            val fileObj = s3Service.getObject(bucketName, key)
            CommonUtil.copyFile(fileObj.getDataInputStream(), localPath, file);
        }
    }

    def getObjectDetails(bucketName: String, key: String): Map[String, AnyRef] = {

        try {
            val s3object = s3Service.getObjectDetails(bucketName, key);
            val bucket = s3Service.getBucket(bucketName);
            Map(
                "ETag" -> s3object.getETag,
                "size" -> s3object.getContentLength.asInstanceOf[AnyRef],
                "createdDate" -> s3object.getLastModifiedDate);
        } catch {
            case ex: S3ServiceException =>
                println("Key not found in the given bucket", bucketName, key);
                JobLogger.log("Key not found in the given bucket", Option(Map("bucket" -> bucketName, "key" -> key)), ERROR);
                Map();
        }
    }

    def deleteObject(bucketName: String, key: String) = {
        try {
            s3Service.deleteObject(bucketName, key)
        } catch {
            case ex: S3ServiceException =>
                JobLogger.log("Key not found in the given bucket", Option(Map("bucket" -> bucketName, "key" -> key)), ERROR);
        }
    }

    def getObject(bucketName: String, key: String): Array[String] = {

        try {
            val fileObj = s3Service.getObject(bucketName, key);
            scala.io.Source.fromInputStream(fileObj.getDataInputStream()).getLines().toArray
        } catch {
            case ex: S3ServiceException =>
                println("Key not found in the given bucket", bucketName, key);
                JobLogger.log("Key not found in the given bucket", Option(Map("bucket" -> bucketName, "key" -> key)), ERROR);
                Array();
        }
    }

    def downloadFile(bucketName: String, key: String, localPath: String, filePrefix: String = "") {

        try {
            val fileObj = s3Service.getObject(bucketName, key)
            val file = filePrefix + key.split("/").last
            CommonUtil.copyFile(fileObj.getDataInputStream(), localPath, file);
        } catch {
            case ex: S3ServiceException =>
                println("Key not found in the given bucket", bucketName, key);
                JobLogger.log("Key not found in the given bucket", Option(Map("bucket" -> bucketName, "key" -> key)), ERROR)
        }
    }

    def uploadPublic(bucketName: String, filePath: String, key: String) {

        val bucketAcl = s3Service.getBucketAcl(bucketName);
        val acl = new AccessControlList();
        acl.setOwner(bucketAcl.getOwner);
        acl.grantPermission(GroupGrantee.ALL_USERS, Permission.PERMISSION_READ);
        val s3Object = new S3Object(new File(filePath));
        s3Object.setKey(key)
        s3Object.setAcl(acl);
        val fileObj = s3Service.putObject(bucketName, s3Object);
        JobLogger.log("File upload successful", Option(Map("etag" -> fileObj.getETag)))
    }

    def getAllKeys(bucketName: String, prefix: String): Array[String] = {
        val s3Objects = s3Service.listObjects(bucketName, prefix, null);
        s3Objects.map { x => x.getKey }
    }

    def search(bucketName: String, prefix: String, fromDate: Option[String] = None, toDate: Option[String] = None, delta: Option[Int] = None, pattern: String = "yyyy-MM-dd"): Array[String] = {
        val from = if (delta.nonEmpty) CommonUtil.getStartDate(toDate, delta.get) else fromDate;
        if (from.nonEmpty) {
            val dates = CommonUtil.getDatesBetween(from.get, toDate, pattern);
            val paths = for (date <- dates) yield {
                getPath(bucketName, prefix + date);
            }
            paths.flatMap { x => x.map { x => x } };
        } else {
            getPath(bucketName, prefix);
        }
    }

    def getPath(bucket: String, prefix: String): Array[String] = {
        S3Util.getAllKeys(bucket, prefix).map { x => "s3n://" + bucket + "/" + x };
    }

}