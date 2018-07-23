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
import java.util.Calendar
import java.util.TimeZone
import org.apache.commons.io.FilenameUtils
import org.apache.commons.lang3.StringUtils
import com.amazonaws.services.s3.AmazonS3Client
import com.typesafe.config.Config
import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest
import com.amazonaws.HttpMethod
import java.util.Date
import com.amazonaws.SDKGlobalConfiguration
import com.amazonaws.regions.Region
import com.amazonaws.regions.Regions
import org.apache.commons.lang.time.DateUtils
import org.joda.time.format.DateTimeFormatter
import org.joda.time.format.DateTimeFormat

object S3Util {

    implicit val className = "org.ekstep.analytics.framework.util.S3Util"

    private val awsCredentials = new AWSCredentials(AppConf.getAwsKey(), AppConf.getAwsSecret());
    private val s3Service = new RestS3Service(awsCredentials);

    private val signatureVersion = AppConf.getConfig("storage-service.request-signature-version");
    private val storageRegion = AppConf.getConfig("s3service.region");

    def upload(bucketName: String, filePath: String, key: String) {
        uploadWithRetries(bucketName, filePath, key)
    }

    def uploadWithRetries(bucketName: String, filePath: String, key: String, attempt: Integer = 1, maxAttempts: Integer = 20): Unit = {
        if (attempt == maxAttempts) {
            val message = s"Failed to upload. filePath: $filePath, key: $key, attempt: $attempt, maxAttempts: $maxAttempts. Exceeded maximum number of retries"
            throw new S3ServiceException(message)
        }

        JobLogger.log("Uploading file to S3. Bucket",
            Option(Map("bucketName" -> bucketName, "FilePath" -> filePath, "attempt" -> attempt,
                "maxAttempts" -> maxAttempts)))

        try {
            val s3Object = new S3Object(new File(filePath));
            s3Object.setKey(key)
            val fileObj = s3Service.putObject(bucketName, s3Object);
            JobLogger.log("File upload successful", Option(Map("etag" -> fileObj.getETag, "attempt" -> attempt)))
        }
        catch {
            case e: Exception => {
                JobLogger.log("Error uploading. Will retry after sometime. ",
                    Option(Map("bucketName" -> bucketName,
                        "FilePath" -> filePath, "exception" -> e.getMessage, "errMsg" -> e.toString(),
                        "attempt" -> attempt, "maxAttempts" -> maxAttempts)), INFO)
                Thread.sleep(attempt*2000)
                uploadWithRetries(bucketName, filePath, key, attempt + 1, maxAttempts)
            }
        }
    }

    def getPreSignedURL(bucketName: String, key: String, expiryTimeInSecs: Long): String = {

        s3Service.createSignedUrlUsingSignatureVersion(signatureVersion, storageRegion, "GET", bucketName, key, null, null, expiryTimeInSecs, false, true, false);
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

    def downloadDirectory(bucketName: String, prefix: String, localPath: String) {
        val objectArr = s3Service.listObjects(bucketName, prefix, null)
        val objects = getAllKeys(bucketName, prefix)
        for (obj <- objectArr) {
            val key = obj.getKey
            val file = FilenameUtils.getName(key);
            val fileObj = s3Service.getObject(bucketName, key)
            val downloadPath = localPath + StringUtils.replace(FilenameUtils.getPath(key), prefix, "") + "/";
            CommonUtil.copyFile(fileObj.getDataInputStream(), downloadPath.replaceAll("//", "/"), file);
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

    def deleteFolder(bucketName: String, folder: String) = {
        try {
            val s3Objects = s3Service.listObjects(bucketName, folder, null);
            s3Objects.foreach { x =>
                s3Service.deleteObject(bucketName, x.getKey)
            }
        } catch {
            case ex: S3ServiceException =>
                ex.printStackTrace();
                JobLogger.log("Unable to delete folder", Option(Map("bucket" -> bucketName, "folder" -> folder)), ERROR);
        }
    }

    def getObject(bucketName: String, key: String): Array[String] = {

        try {
            val fileObj = s3Service.getObject(bucketName, key);
            scala.io.Source.fromInputStream(fileObj.getDataInputStream()).getLines().toArray
        } catch {
            case ex: S3ServiceException =>
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
                JobLogger.log("Key not found in the given bucket", Option(Map("bucket" -> bucketName, "key" -> key)), ERROR)
        }
    }

    def uploadPublic(bucketName: String, filePath: String, key: String) {

        val acl = new AccessControlList();
        acl.setOwner(s3Service.getBucket(bucketName).getOwner);
        acl.grantPermission(GroupGrantee.ALL_USERS, Permission.PERMISSION_READ);
        val s3Object = new S3Object(new File(filePath));
        s3Object.setKey(key)
        s3Object.setAcl(acl);
        val fileObj = s3Service.putObject(bucketName, s3Object);
        JobLogger.log("File upload successful", Option(Map("etag" -> fileObj.getETag)))
    }

    def uploadPublicWithExpiry(bucketName: String, filePath: String, key: String, expiryInDays: Int): String = {

        val s3Object = new S3Object(new File(filePath));
        val fileObj = s3Service.putObject(bucketName, s3Object);
        val cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
        cal.add(Calendar.DAY_OF_YEAR, expiryInDays);
        val expiryDate = cal.getTime();
        val signedUrl = s3Service.createSignedGetUrl(bucketName, fileObj.getKey(), expiryDate, false);
        JobLogger.log("File upload successful", Option(Map("etag" -> fileObj.getETag, "signedUrl" -> signedUrl)));
        signedUrl;
    }

    def getAllKeys(bucketName: String, prefix: String): Array[String] = {
        val s3Objects = s3Service.listObjects(bucketName, prefix, null);
        s3Objects.map { x => x.getKey }
    }
    
    def searchKeys(bucketName: String, prefix: String, fromDate: Option[String] = None, toDate: Option[String] = None, delta: Option[Int] = None, pattern: String = "yyyy-MM-dd"): Array[String] = {
        val from = if (delta.nonEmpty) CommonUtil.getStartDate(toDate, delta.get) else fromDate;
        if (from.nonEmpty) {
            val dates = CommonUtil.getDatesBetween(from.get, toDate, pattern);
            val paths = for (date <- dates) yield {
                S3Util.getAllKeys(bucketName, prefix + date)
            }
            paths.flatMap { x => x.map { x => x } };
        } else {
            S3Util.getAllKeys(bucketName, prefix)
        }
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
    
    def searchByCreatedDate(bucketName: String, prefix: String, creationDate: String, pattern: String = "yyyy-MM-dd"): Array[String] = {
        val df: DateTimeFormatter = DateTimeFormat.forPattern(pattern).withZoneUTC();
        val createdDate = df.parseLocalDate(creationDate);
        val objs = s3Service.listObjects(bucketName, prefix, null);
        
        objs.filter(p => DateUtils.isSameDay(p.getLastModifiedDate, createdDate.toDate())).map { x => "s3n://" + bucketName + "/" + x.getKey };
    }

    def getPath(bucket: String, prefix: String): Array[String] = {
        S3Util.getAllKeys(bucket, prefix).map { x => "s3n://" + bucket + "/" + x };
    }
    
}