package org.ekstep.analytics.util

object CypherQueries {

    /**
     * Content Snapshot Summarizer Cypher Query
     */
    // For author = partner = all
    val CONTENT_SNAPSHOT_TOTAL_USER_COUNT = "MATCH (usr :User {type:'author'}) RETURN usr.appid AS appId, usr.channel AS channel, count(usr) AS userCount" //"MATCH (usr :User {type:'author'}) RETURN count(usr)"
    val CONTENT_SNAPSHOT_ACTIVE_USER_COUNT = "MATCH (usr:User {type:'author'})<-[r:createdBy]-(cnt: domain{IL_FUNC_OBJECT_TYPE:'Content'}) WHERE lower(cnt.contentType) IN ['story', 'game', 'collection', 'worksheet'] RETURN usr.appid AS appId, usr.channel AS channel, usr.IL_UNIQUE_ID, cnt.createdOn" //"MATCH (usr:User {type:'author'})<-[r:createdBy]-(cnt: domain{IL_FUNC_OBJECT_TYPE:'Content'}) WHERE lower(cnt.contentType) IN ['story', 'game', 'collection', 'worksheet'] RETURN usr.IL_UNIQUE_ID, cnt.createdOn"
    val CONTENT_COUNT_BY_STATUS = "MATCH (cnt: domain{IL_FUNC_OBJECT_TYPE:'Content'}) WHERE lower(cnt.contentType) IN ['story', 'game', 'collection', 'worksheet'] RETURN cnt.appid AS appId, cnt.channel AS channel, lower(cnt.status) AS status, count(cnt) AS count" //"MATCH (cnt: domain{IL_FUNC_OBJECT_TYPE:'Content'}) WHERE lower(cnt.contentType) IN ['story', 'game', 'collection', 'worksheet'] RETURN lower(cnt.status) AS status, count(cnt) AS count";

    // For specific author and partner = all
    val CONTENT_COUNT_PER_AUTHOR_BY_STATUS = "MATCH (usr:User {type:'author'})<-[r:createdBy]-(cnt: domain{IL_FUNC_OBJECT_TYPE:'Content'}) WHERE lower(cnt.contentType) IN ['story', 'game', 'collection', 'worksheet'] WITH usr, cnt RETURN usr.appid AS appId, usr.channel AS channel, usr.IL_UNIQUE_ID AS identifier, lower(cnt.status) AS status, count(cnt) AS count" //"MATCH (usr:User {type:'author'})<-[r:createdBy]-(cnt: domain{IL_FUNC_OBJECT_TYPE:'Content'}) WHERE lower(cnt.contentType) IN ['story', 'game', 'collection', 'worksheet'] WITH usr, cnt RETURN usr.IL_UNIQUE_ID AS identifier, lower(cnt.status) AS status, count(cnt) AS count"
    val AUTHOR_CONTENT_LIST = "MATCH (usr:User {type:'author'})<-[r:createdBy]-(cnt: domain{IL_FUNC_OBJECT_TYPE:'Content'}) WHERE lower(cnt.contentType) IN ['story', 'game', 'collection', 'worksheet'] with usr, usr.IL_UNIQUE_ID AS author, collect(cnt.IL_UNIQUE_ID) as contentList RETURN usr.appid AS appId, usr.channel AS channel, author, contentList" //"MATCH (usr:User {type:'author'})<-[r:createdBy]-(cnt: domain{IL_FUNC_OBJECT_TYPE:'Content'}) WHERE lower(cnt.contentType) IN ['story', 'game', 'collection', 'worksheet'] with usr, usr.IL_UNIQUE_ID AS author, collect(cnt.IL_UNIQUE_ID) as contentList RETURN author, contentList"

    //For specific partner and author = all
    val CONTENT_SNAPSHOT_PARTNER_USER_COUNT = "MATCH (usr:User {type:'author'})<-[r:createdBy]-(cnt: domain{IL_FUNC_OBJECT_TYPE:'Content'}) WHERE lower(cnt.contentType) IN ['story', 'game', 'collection', 'worksheet'] AND EXISTS(cnt.createdFor) RETURN usr.appid AS appId, usr.channel AS channel, usr.IL_UNIQUE_ID, cnt.createdFor, cnt.createdOn" //"MATCH (usr:User {type:'author'})<-[r:createdBy]-(cnt: domain{IL_FUNC_OBJECT_TYPE:'Content'}) WHERE lower(cnt.contentType) IN ['story', 'game', 'collection', 'worksheet'] AND EXISTS(cnt.createdFor) RETURN usr.IL_UNIQUE_ID, cnt.createdFor, cnt.createdOn"
    val CONTENT_COUNT_PER_PARTNER_BY_STATUS = "MATCH (cnt: domain{IL_FUNC_OBJECT_TYPE:'Content'}) WHERE lower(cnt.contentType) IN ['story', 'game', 'collection', 'worksheet'] AND EXISTS(cnt.createdFor) WITH cnt RETURN cnt.appid AS appId, cnt.channel AS channel, cnt.createdFor AS identifier, lower(cnt.status) AS status, count(cnt) AS count" //"MATCH (cnt: domain{IL_FUNC_OBJECT_TYPE:'Content'}) WHERE lower(cnt.contentType) IN ['story', 'game', 'collection', 'worksheet'] AND EXISTS(cnt.createdFor) WITH cnt RETURN cnt.createdFor AS identifier, lower(cnt.status) AS status, count(cnt) AS count"
    val PARTNER_CONTENT_LIST = "MATCH (cnt: domain{IL_FUNC_OBJECT_TYPE:'Content'}) WHERE lower(cnt.contentType) IN ['story', 'game', 'collection', 'worksheet'] AND EXISTS(cnt.createdFor) with cnt UNWIND cnt.createdFor AS partner RETURN cnt.appid AS appId, cnt.channel AS channel, partner, collect(cnt.IL_UNIQUE_ID) AS contentList" // "MATCH (cnt: domain{IL_FUNC_OBJECT_TYPE:'Content'}) WHERE lower(cnt.contentType) IN ['story', 'game', 'collection', 'worksheet'] AND EXISTS(cnt.createdFor) with cnt UNWIND cnt.createdFor AS partner RETURN partner, collect(cnt.IL_UNIQUE_ID) AS contentList"

    // For specific author and partner
    val CONTENT_COUNT_PER_AUTHOR_PER_PARTNER_BY_STATUS = "MATCH (usr:User {type:'author'})<-[r:createdBy]-(cnt: domain{IL_FUNC_OBJECT_TYPE:'Content'}) WHERE lower(cnt.contentType) IN ['story', 'game', 'collection', 'worksheet'] AND EXISTS(cnt.createdFor) WITH usr, cnt RETURN cnt.appid AS appId, cnt.channel AS channel, usr.IL_UNIQUE_ID AS author, cnt.createdFor AS partner, lower(cnt.status) AS status, count(cnt) AS count" //"MATCH (usr:User {type:'author'})<-[r:createdBy]-(cnt: domain{IL_FUNC_OBJECT_TYPE:'Content'}) WHERE lower(cnt.contentType) IN ['story', 'game', 'collection', 'worksheet'] AND EXISTS(cnt.createdFor) WITH usr, cnt RETURN usr.IL_UNIQUE_ID AS author, cnt.createdFor AS partner, lower(cnt.status) AS status, count(cnt) AS count"
    val AUTHOR_PARTNER_CONTENT_LIST = "MATCH (usr:User {type:'author'})<-[r:createdBy]-(cnt: domain{IL_FUNC_OBJECT_TYPE:'Content'}) WHERE lower(cnt.contentType) IN ['story', 'game', 'collection', 'worksheet'] AND EXISTS(cnt.createdFor) WITH usr, cnt UNWIND cnt.createdFor AS partner RETURN cnt.appid AS appId, cnt.channel AS channel, usr.IL_UNIQUE_ID AS author, partner, collect(cnt.IL_UNIQUE_ID) AS contentList" //"MATCH (usr:User {type:'author'})<-[r:createdBy]-(cnt: domain{IL_FUNC_OBJECT_TYPE:'Content'}) WHERE lower(cnt.contentType) IN ['story', 'game', 'collection', 'worksheet'] AND EXISTS(cnt.createdFor) WITH usr, cnt UNWIND cnt.createdFor AS partner RETURN usr.IL_UNIQUE_ID AS author, partner, collect(cnt.IL_UNIQUE_ID) AS contentList"

    /**
     * Concept Snapshot Summarizer Cypher Query
     */

    val CONCEPT_SNAPSHOT_TOTAL_CONTENT_COUNT = "MATCH (cnc:domain{IL_FUNC_OBJECT_TYPE:'Concept'}) RETURN cnc.IL_UNIQUE_ID AS identifier, cnc.appid as appId, cnc.channel as channel, cnc.contentCount AS count"
    val CONCEPT_SNAPSHOT_REVIEW_CONTENT_COUNT = "MATCH (cnt:domain{IL_FUNC_OBJECT_TYPE:'Content'})-[r:associatedTo]->(cnc:domain{IL_FUNC_OBJECT_TYPE:'Concept'}) WHERE lower(cnt.contentType) IN ['story', 'game', 'collection', 'worksheet'] AND cnt.status='Review' WITH cnc, count(r) AS count RETURN cnc.IL_UNIQUE_ID AS identifier, cnc.appid as appId, cnc.channel as channel, count"
    val CONCEPT_SNAPSHOT_LIVE_CONTENT_COUNT = "MATCH (cnc:domain{IL_FUNC_OBJECT_TYPE:'Concept'}) RETURN cnc.IL_UNIQUE_ID AS identifier, cnc.appid as appId, cnc.channel as channel, cnc.liveContentCount AS count"

    /**
     * Asset Snapshot Summarizer Cypher Query
     *
     */

    val ASSET_SNAP_MEDIA_TOTAL = "MATCH (ast:domain{IL_FUNC_OBJECT_TYPE:'Content',contentType:'Asset'}) RETURN ast.appid as appId, ast.channel as channel, ast.mediaType as mediaType, count(ast.IL_UNIQUE_ID) as count" //"MATCH (ast:domain{IL_FUNC_OBJECT_TYPE:'Content',contentType:'Asset'}) RETURN ast.mediaType as mediaType, count(ast.IL_UNIQUE_ID) as count"
    val ASSET_SNAP_MEDIA_USED = "MATCH p=(cnt:domain{IL_FUNC_OBJECT_TYPE:'Content'})-[r:uses]->(ast:domain{IL_FUNC_OBJECT_TYPE:'Content',contentType:'Asset'}) RETURN ast.appid as appId, ast.channel as channel, ast.mediaType as mediaType, count(distinct ast.IL_UNIQUE_ID) as count" //"MATCH p=(cnt:domain{IL_FUNC_OBJECT_TYPE:'Content'})-[r:uses]->(ast:domain{IL_FUNC_OBJECT_TYPE:'Content',contentType:'Asset'}) RETURN ast.mediaType as mediaType, count(distinct ast.IL_UNIQUE_ID) as count"
    val ASSET_SNAP_TOTAL_QUESTION = "MATCH (as: domain {IL_FUNC_OBJECT_TYPE:'AssessmentItem'}) return as.appid as appId, as.channel as channel, count(as) as count" //"match (as: domain {IL_FUNC_OBJECT_TYPE:'AssessmentItem'}) return count(as) as count"
    val ASSET_SNAP_USED_QUESTION = "MATCH (cnt: domain{IL_FUNC_OBJECT_TYPE:'Content'}) - [r1: associatedTo] -> (is: domain{IL_FUNC_OBJECT_TYPE:'ItemSet'}) - [r2: hasMember] -> (as: domain{IL_FUNC_OBJECT_TYPE:'AssessmentItem'}) WHERE lower(cnt.contentType) IN ['story', 'game', 'collection', 'worksheet'] RETURN as.appid as appId, as.channel as channel, count(distinct as) as count" //"MATCH (cnt: domain{IL_FUNC_OBJECT_TYPE:'Content'}) - [r1: associatedTo] -> (is: domain{IL_FUNC_OBJECT_TYPE:'ItemSet'}) - [r2: hasMember] -> (as: domain{IL_FUNC_OBJECT_TYPE:'AssessmentItem'}) WHERE lower(cnt.contentType) IN ['story', 'game', 'collection', 'worksheet'] RETURN count(distinct as) as count"
    val ASSET_SNAP_TOTAL_ACTIVITIES = "MATCH (act: domain {IL_FUNC_OBJECT_TYPE:'Content', contentType: 'Plugin'}) RETURN act.appid as appId, act.channel as channel, count(act) as count" //"match (act: domain {IL_FUNC_OBJECT_TYPE:'Content', contentType: 'Plugin'}) return count(act) as count"
    val ASSET_SNAP_USED_ACTIVITIES = "MATCH (cnt: domain {IL_FUNC_OBJECT_TYPE: 'Content'}) -[r: uses]-> (act: domain {IL_FUNC_OBJECT_TYPE:'Content', contentType: 'Plugin'}) WHERE lower(cnt.contentType) IN ['story', 'game', 'collection', 'worksheet'] RETURN act.appid as appId, act.channel as channel, count(distinct act) as count" //"match (cnt: domain {IL_FUNC_OBJECT_TYPE: 'Content'}) -[r: uses]-> (act: domain {IL_FUNC_OBJECT_TYPE:'Content', contentType: 'Plugin'}) WHERE lower(cnt.contentType) IN ['story', 'game', 'collection', 'worksheet'] return count(distinct act) as count"
    val ASSET_SNAP_TOTAL_TEMPLATES = "MATCH (temp: domain {IL_FUNC_OBJECT_TYPE:'Content', contentType: 'Template'}) RETURN temp.appid as appId, temp.channel as channel, count(temp) as count" //"match (temp: domain {IL_FUNC_OBJECT_TYPE:'Content', contentType: 'Template'}) return count(temp) as count"
    val ASSET_SNAP_USED_TEMPLATES = "MATCH (temp: domain{IL_FUNC_OBJECT_TYPE:'Content', contentType: 'Template'}) - [r: associatedTo] - (cnc: domain{IL_FUNC_OBJECT_TYPE:'Concept'}) WHERE cnc.contentCount > 0 RETURN temp.appid as appId, temp.channel as channel, count(distinct temp) as count" //"MATCH (temp: domain{IL_FUNC_OBJECT_TYPE:'Content', contentType: 'Template'}) - [r: associatedTo] - (cnc: domain{IL_FUNC_OBJECT_TYPE:'Concept'}) WHERE cnc.contentCount > 0 RETURN count(distinct temp) as count"

    /**
     * Plugin Snapshot Matrix Cypher Query
     *
     */

    val PLUGIN_SNAPSHOT_METIRCS = """MATCH (p:domain{IL_FUNC_OBJECT_TYPE:'Content', contentType:'Plugin'}) OPTIONAL MATCH (p)<-[r:uses]-(n:domain{IL_FUNC_OBJECT_TYPE:'Content'}) WHERE lower(n.contentType) in ["game", "worksheet", "story", "collection"] RETURN p.IL_UNIQUE_ID as plugin_id, p.name as name, CASE WHEN p.category IS null THEN [] ELSE p.category END as category, CASE WHEN p.createdBy IS null THEN "" ELSE p.createdBy END as author, count(n) as contentCount"""

    /**
     * Create Relationship AssessmentItem and Template
     *
     */
    val TEMPLATE_ASSES_REL_CREATION = """MATCH (q: domain {IL_FUNC_OBJECT_TYPE:'AssessmentItem'}), (temp:domain{IL_FUNC_OBJECT_TYPE:'Content', contentType: 'Template'}) WHERE q.template = temp.IL_UNIQUE_ID MERGE (q)-[r:uses]->(temp)"""
    /**
     * Create Relationship Content and Template
     *
     */

    val TEMPLATE_CONTENT_REL_CREATION = """MATCH (cnt:domain{IL_FUNC_OBJECT_TYPE:'Content'}), (temp:domain{IL_FUNC_OBJECT_TYPE:'Content', contentType: 'Template'}) WHERE cnt.template = temp.IL_UNIQUE_ID MERGE (cnt)-[r:uses]->(temp)"""
    /**
     * Template Snapshot Matrix Cypher Query
     *
     */
    val TEMPLATE_SNAPSHOT_METIRCS = """MATCH (temp:domain{IL_FUNC_OBJECT_TYPE:'Content', contentType:'Template'}) OPTIONAL MATCH (temp)<-[r:uses]-(cnt:domain{IL_FUNC_OBJECT_TYPE:'Content'}) WHERE lower(cnt.contentType) in ["game", "worksheet", "story", "collection"] with temp, count(cnt) as contentCount OPTIONAL MATCH (temp)<-[re:uses]-(q:domain{IL_FUNC_OBJECT_TYPE:'AssessmentItem'}) with temp, contentCount, count(q) as questionCount RETURN temp.IL_UNIQUE_ID as template_id, temp.name as name, CASE WHEN temp.category IS null THEN [] ELSE temp.category END as category, CASE WHEN temp.createdBy IS null THEN "" ELSE temp.createdBy END as author, contentCount, questionCount""" //"""MATCH (temp:domain{IL_FUNC_OBJECT_TYPE:'Content', contentType:'Template'}) OPTIONAL MATCH (temp)<-[r:uses]-(cnt:domain{IL_FUNC_OBJECT_TYPE:'Content'}) WHERE lower(cnt.contentType) in ["game", "worksheet", "story", "collection"] OPTIONAL MATCH (temp)<-[re:uses]-(q:domain{IL_FUNC_OBJECT_TYPE:'AssessmentItem'})  RETURN temp.IL_UNIQUE_ID as template_id, temp.name as name, CASE WHEN temp.category IS null THEN [] ELSE temp.category END as category, CASE WHEN temp.createdBy IS null THEN "" ELSE temp.createdBy END as author, count(cnt) as contentCount,count(q) as questionCount"""

    /**
     * Content Creation Metrics Cypher Query
     *
     */
    val PER_CONTENT_TAGS = "match (e: domain{IL_SYS_NODE_TYPE:'TAG'})-[r: hasMember]-> (cnt: domain {IL_FUNC_OBJECT_TYPE:'Content'}) WHERE lower(cnt.contentType) IN ['story', 'game', 'collection', 'worksheet'] return cnt.IL_UNIQUE_ID as contentId, count(e) as tagCount"
    val CONTENT_LIVE_COUNT = "match (cnt:domain{IL_FUNC_OBJECT_TYPE:'Content'}) WHERE lower(cnt.contentType) IN ['story', 'game', 'collection', 'worksheet'] return cnt.IL_UNIQUE_ID as contentId, CASE WHEN cnt.pkgVersion IS null THEN 0 ELSE cnt.pkgVersion END AS liveCount"

    /**
     * Textbook Snapshot Summary Queries
     */

    val TEXTBOOK_SNAPSHOT_UNIT_COUNT = "MATCH (txtbk:domain{IL_FUNC_OBJECT_TYPE:'Content', contentType:'Textbook'}) WHERE txtbk.status<>'Retired' OPTIONAL MATCH p=(txtbk)-[r:hasSequenceMember*..10]->(txtbkUnit:domain{IL_FUNC_OBJECT_TYPE:'Content', contentType:'TextBookUnit'}) WITH txtbk, CASE WHEN p is null THEN 0 ELSE COUNT(txtbkUnit) END AS textbookunit_count RETURN txtbk.IL_UNIQUE_ID AS identifier, txtbk.appid AS appId, txtbk.channel AS channel, txtbk.status AS status, CASE WHEN txtbk.createdBy is null THEN '' ELSE txtbk.createdBy END AS author_id, CASE WHEN txtbk.board is null THEN '' ELSE txtbk.board END AS board, CASE WHEN txtbk.medium is null THEN '' ELSE txtbk.medium END AS medium, CASE WHEN txtbk.collaborators is null THEN 0 ELSE count(txtbk.collaborators) END AS creators_count, textbookunit_count" //"MATCH (txtbk:domain{IL_FUNC_OBJECT_TYPE:'Content', contentType:'Textbook'}) WHERE txtbk.status<>'Retired' OPTIONAL MATCH p=(txtbk)-[r:hasSequenceMember*..10]->(txtbkUnit:domain{IL_FUNC_OBJECT_TYPE:'Content', contentType:'TextBookUnit'}) WITH txtbk, CASE WHEN p is null THEN 0 ELSE COUNT(txtbkUnit) END AS textbookunit_count RETURN txtbk.IL_UNIQUE_ID AS identifier, txtbk.status AS status, CASE WHEN txtbk.createdBy is null THEN '' ELSE txtbk.createdBy END AS author_id, CASE WHEN txtbk.board is null THEN '' ELSE txtbk.board END AS board, CASE WHEN txtbk.medium is null THEN '' ELSE txtbk.medium END AS medium, CASE WHEN txtbk.collaborators is null THEN 0 ELSE count(txtbk.collaborators) END AS creators_count, textbookunit_count";
    val TEXTBOOK_SNAPSHOT_CONTENT_COUNT = "MATCH (txtbk:domain{IL_FUNC_OBJECT_TYPE:'Content', contentType:'Textbook'}) WHERE txtbk.status<>'Retired' OPTIONAL MATCH p=(txtbk)-[r:hasSequenceMember*..10]->(cnt:domain{IL_FUNC_OBJECT_TYPE:'Content'}) WHERE cnt.contentType<>'TextBookUnit' WITH txtbk, CASE WHEN p is null THEN 0 ELSE COUNT(cnt) END AS content_count, CASE WHEN p is null THEN [] ELSE COLLECT(DISTINCT cnt.contentType) END AS content_types RETURN txtbk.IL_UNIQUE_ID AS identifier, txtbk.appid AS appId, txtbk.channel AS channel, content_count, content_types" //"MATCH (txtbk:domain{IL_FUNC_OBJECT_TYPE:'Content', contentType:'Textbook'}) WHERE txtbk.status<>'Retired' OPTIONAL MATCH p=(txtbk)-[r:hasSequenceMember*..10]->(cnt:domain{IL_FUNC_OBJECT_TYPE:'Content'}) WHERE cnt.contentType<>'TextBookUnit' WITH txtbk, CASE WHEN p is null THEN 0 ELSE COUNT(cnt) END AS content_count, CASE WHEN p is null THEN [] ELSE COLLECT(DISTINCT cnt.contentType) END AS content_types RETURN txtbk.IL_UNIQUE_ID AS identifier, content_count, content_types";
}