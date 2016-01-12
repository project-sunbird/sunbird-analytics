package org.ekstep.analytics.framework.adapter

import scala.collection.mutable.Buffer
import org.ekstep.analytics.framework.util.AppDBUtils
import org.apache.commons.dbutils.QueryRunner
import org.apache.commons.dbutils.ResultSetHandler
import java.sql.ResultSet
import scala.collection.mutable.ListBuffer
import scala.collection.immutable.Map
import scala.collection.mutable.HashMap
import org.ekstep.analytics.framework.User
import org.ekstep.analytics.framework.UserProfile

/**
 * @author Santhosh
 */
object UserAdapter {

    private val userProfileResultHandler = new ResultSetHandler[Map[String, UserProfile]]() {

        override def handle(rs: ResultSet): Map[String, UserProfile] = {
            if (!rs.next()) {
                return null;
            }

            var result = new HashMap[String, UserProfile]();
            var i = 0;

            do {
                result(rs.getString(1)) = UserProfile(rs.getString(1), rs.getString(3), rs.getInt(4));
            } while (rs.next())

            result.toMap;
        }
    };

    private val languageResultHandler = new ResultSetHandler[Map[Int, String]]() {

        override def handle(rs: ResultSet): Map[Int, String] = {
            if (!rs.next()) {
                return null;
            }

            var result = new HashMap[Int, String]();
            var i = 0;

            do {
                result(rs.getInt(1)) = rs.getString(2);
            } while (rs.next())

            result.toMap;
        }
    };

    
    def getUserProfileMapping(): Map[String, UserProfile] = {
        val conn = AppDBUtils.getConnection;
        val qr = new QueryRunner();
        try {
            qr.query(conn, "SELECT uid, handle, gender, age, standard, language FROM profile", userProfileResultHandler);
        } finally {
            AppDBUtils.closeConnection(conn);            
        }
    }
    
    def getUserProfileMapping(userId: String): UserProfile = {
        val conn = AppDBUtils.getConnection;
        val qr = new QueryRunner();
        try {
            val results = qr.query(conn, "SELECT uid, handle, gender, age, standard, language FROM profile where uid = '" + userId + "'", userProfileResultHandler);
            if(results == null) return null;
            results.getOrElse(userId, null);
        } finally {
            AppDBUtils.closeConnection(conn);            
        }
    }

    def getLanguageMapping(): Map[Int, String] = {
        val conn = AppDBUtils.getConnection;
        val qr = new QueryRunner();
        try {
            qr.query(conn, "select * from languages", languageResultHandler);
        } finally {
            AppDBUtils.closeConnection(conn);    
        }
    }

}