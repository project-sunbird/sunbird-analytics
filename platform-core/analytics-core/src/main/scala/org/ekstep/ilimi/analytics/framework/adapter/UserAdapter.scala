package org.ekstep.ilimi.analytics.framework.adapter

import scala.collection.mutable.Buffer
import org.ekstep.ilimi.analytics.framework.util.AppDBUtils
import org.apache.commons.dbutils.QueryRunner
import org.apache.commons.dbutils.ResultSetHandler
import java.sql.ResultSet
import scala.collection.mutable.ListBuffer
import scala.collection.immutable.Map
import scala.collection.mutable.HashMap
import org.ekstep.ilimi.analytics.framework.User
import org.ekstep.ilimi.analytics.framework.UserProfile

/**
 * @author Santhosh
 */
object UserAdapter {

    private val userResultHandler = new ResultSetHandler[Map[String, User]]() {

        override def handle(rs: ResultSet): Map[String, User] = {
            if (!rs.next()) {
                return null;
            }

            var result = new HashMap[String, User]();
            var i = 0;

            do {
                result(rs.getString(2)) = User(rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(5), rs.getDate(4), rs.getInt(6));
            } while (rs.next())

            result.toMap;
        }
    };
    
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

    def getUserMapping(): Map[String, User] = {
        val conn = AppDBUtils.getConnection;
        val qr = new QueryRunner();
        val results = qr.query(conn, "SELECT name, encoded_id, ekstep_id, dob, gender, language_id FROM children", userResultHandler);
        AppDBUtils.closeConnection(conn);
        results;
    }
    
    def getUserProfileMapping(): Map[String, UserProfile] = {
        val conn = AppDBUtils.getConnection;
        val qr = new QueryRunner();
        val results = qr.query(conn, "SELECT uid, handle, gender, age, standard, language FROM profile", userProfileResultHandler);
        AppDBUtils.closeConnection(conn);
        results;
    }

    def getLanguageMapping(): Map[Int, String] = {
        val conn = AppDBUtils.getConnection;
        val qr = new QueryRunner();
        val results = qr.query(conn, "select * from languages", languageResultHandler);
        AppDBUtils.closeConnection(conn);
        results;
    }

}