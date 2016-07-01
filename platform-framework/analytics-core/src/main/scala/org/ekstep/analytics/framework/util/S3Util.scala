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

object S3Util {

    val className = "org.ekstep.analytics.framework.util.S3Util"

    private val awsCredentials = new AWSCredentials(AppConf.getAwsKey(), AppConf.getAwsSecret());
    private val s3Service = new RestS3Service(awsCredentials);

    def upload(bucketName: String, filePath: String, key: String) {
        JobLogger.log("Uploading file to S3. Bucket", className, None, Option(Map("bucketName" -> bucketName, "FilePath" -> filePath)), None, "DEBUG")
        val s3Object = new S3Object(new File(filePath));
        s3Object.setKey(key)
        val fileObj = s3Service.putObject(bucketName, s3Object);
        JobLogger.log("File upload successful", className, None, Option(Map("etag" -> fileObj.getETag)), None, "DEBUG")
    }

    /*
    def uploadPublic(bucketName: String, filePath: String, key: String) {

        val bucketAcl = s3Service.getBucketAcl(bucketName);
        val acl = new AccessControlList();
        acl.setOwner(bucketAcl.getOwner);
        acl.grantPermission(GroupGrantee.ALL_USERS, Permission.PERMISSION_READ);
        val s3Object = new S3Object(new File(filePath));
        s3Object.setKey(key)
        s3Object.setAcl(acl);
        val fileObj = s3Service.putObject(bucketName, s3Object);
        JobLogger.debug("File upload successful", className, Option(Map("etag" -> fileObj.getETag)));
    }

    def getMetadata(bucketName: String, key: String) {
        val bucket = s3Service.getBucket(bucketName);
        val s3Object = s3Service.getObjectDetails(bucket, key);
    }
    */

    def getAllKeys(bucketName: String, prefix: String): Array[String] = {
        val bucket = s3Service.getBucket(bucketName);
        val s3Objects = s3Service.listObjects(bucket, prefix, null);
        s3Objects.map { x => x.getKey }
    }

    def search(bucketName: String, prefix: String, fromDate: Option[String] = None, toDate: Option[String] = None, delta: Option[Int] = None): Buffer[String] = {
        var paths = ListBuffer[String]();
        val from = if (delta.nonEmpty) CommonUtil.getStartDate(toDate, delta.get) else fromDate;
        if (from.nonEmpty) {
            val dates = CommonUtil.getDatesBetween(from.get, toDate);
            dates.foreach { x =>
                {
                    paths ++= getPath(bucketName, prefix + x);
                }
            }
        } else {
            paths ++= getPath(bucketName, prefix);
        }
        paths;
    }

    def getPath(bucket: String, prefix: String): Array[String] = {
        S3Util.getAllKeys(bucket, prefix).map { x => "s3n://" + bucket + "/" + x };
    }

}