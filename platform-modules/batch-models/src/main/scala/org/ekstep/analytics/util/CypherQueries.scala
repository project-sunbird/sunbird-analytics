package org.ekstep.analytics.util

object CypherQueries {

    
    /**
     * Content Snapshot Summarizer Cypher Query
     **/
    // For author = partner = all
    val CONTENT_SNAPSHOT_TOTAL_USER_COUNT = "MATCH (usr :User {type:'author'}) RETURN count(usr)"
    val CONTENT_SNAPSHOT_ACTIVE_USER_COUNT = "MATCH (usr:User {type:'author'})<-[r:createdBy]-(cnt: domain{IL_FUNC_OBJECT_TYPE:'Content'}) WHERE lower(cnt.contentType) IN ['story', 'game', 'collection', 'worksheet'] RETURN usr.IL_UNIQUE_ID, cnt.createdOn"
    val CONTENT_COUNT_BY_STATUS = "MATCH (cnt: domain{IL_FUNC_OBJECT_TYPE:'Content'}) WHERE lower(cnt.contentType) IN ['story', 'game', 'collection', 'worksheet'] RETURN lower(cnt.status) AS status, count(cnt) AS count";

    // For specific author and partner = all
    val CONTENT_COUNT_PER_AUTHOR_BY_STATUS = "MATCH (usr:User {type:'author'})<-[r:createdBy]-(cnt: domain{IL_FUNC_OBJECT_TYPE:'Content'}) WHERE lower(cnt.contentType) IN ['story', 'game', 'collection', 'worksheet'] WITH usr, cnt RETURN usr.IL_UNIQUE_ID AS identifier, lower(cnt.status) AS status, count(cnt) AS count"

    //For specific partner and author = all
    val CONTENT_SNAPSHOT_PARTNER_USER_COUNT = "MATCH (usr:User {type:'author'})<-[r:createdBy]-(cnt: domain{IL_FUNC_OBJECT_TYPE:'Content'}) WHERE lower(cnt.contentType) IN ['story', 'game', 'collection', 'worksheet'] AND EXISTS(cnt.createdFor) RETURN usr.IL_UNIQUE_ID, cnt.createdFor, cnt.createdOn"
    val CONTENT_COUNT_PER_PARTNER_BY_STATUS = "MATCH (cnt: domain{IL_FUNC_OBJECT_TYPE:'Content'}) WHERE lower(cnt.contentType) IN ['story', 'game', 'collection', 'worksheet'] AND EXISTS(cnt.createdFor) WITH cnt RETURN cnt.createdFor AS identifier, lower(cnt.status) AS status, count(cnt) AS count"

    // For specific author and partner
    val CONTENT_COUNT_PER_AUTHOR_PER_PARTNER_BY_STATUS = "MATCH (usr:User {type:'author'})<-[r:createdBy]-(cnt: domain{IL_FUNC_OBJECT_TYPE:'Content'}) WHERE lower(cnt.contentType) IN ['story', 'game', 'collection', 'worksheet'] AND EXISTS(cnt.createdFor) WITH usr, cnt RETURN usr.IL_UNIQUE_ID AS author, cnt.createdFor AS partner, lower(cnt.status) AS status, count(cnt) AS count"
    
    /**
     * Concept Snapshot Summarizer Cypher Query
     **/
    
    val CONCEPT_SNAPSHOT_TOTAL_CONTENT_COUNT = "MATCH (cnc:domain{IL_FUNC_OBJECT_TYPE:'Concept'}) RETURN cnc.IL_UNIQUE_ID AS identifier, cnc.contentCount AS count"
    val CONCEPT_SNAPSHOT_REVIEW_CONTENT_COUNT = "MATCH (cnt:domain{IL_FUNC_OBJECT_TYPE:'Content'})-[r:associatedTo]->(cnc:domain{IL_FUNC_OBJECT_TYPE:'Concept'}) WHERE lower(cnt.contentType) IN ['story', 'game', 'collection', 'worksheet'] AND cnt.status='Review' WITH cnc, count(r) AS count RETURN cnc.IL_UNIQUE_ID AS identifier, count"
    val CONCEPT_SNAPSHOT_LIVE_CONTENT_COUNT = "MATCH (cnc:domain{IL_FUNC_OBJECT_TYPE:'Concept'}) RETURN cnc.IL_UNIQUE_ID AS identifier, cnc.liveContentCount AS count"
    
   
    /**
     * Asset Snapshot Summarizer Cypher Query
     * 
     **/
    
    val ASSET_SNAP_MEDIA_TOTAL = "MATCH (ast:domain{IL_FUNC_OBJECT_TYPE:'Content',contentType:'Asset'}) RETURN ast.mediaType as mediaType, count(ast.IL_UNIQUE_ID) as count"
    val ASSET_SNAP_MEDIA_USED = "MATCH p=(cnt:domain{IL_FUNC_OBJECT_TYPE:'Content'})-[r:uses]->(ast:domain{IL_FUNC_OBJECT_TYPE:'Content',contentType:'Asset'}) RETURN ast.mediaType as mediaType, count(distinct ast.IL_UNIQUE_ID) as count"
    val ASSET_SNAP_TOTAL_QUESTION = "match (as: domain {IL_FUNC_OBJECT_TYPE:'AssessmentItem'}) return count(as) as count"
    val ASSET_SNAP_USED_QUESTION = "MATCH (cnt: domain{IL_FUNC_OBJECT_TYPE:'Content'}) - [r1: associatedTo] -> (is: domain{IL_FUNC_OBJECT_TYPE:'ItemSet'}) - [r2: hasMember] -> (as: domain{IL_FUNC_OBJECT_TYPE:'AssessmentItem'}) WHERE lower(cnt.contentType) IN ['story', 'game', 'collection', 'worksheet'] RETURN count(distinct as) as count"
    val ASSET_SNAP_TOTAL_ACTIVITIES = "match (act: domain {IL_FUNC_OBJECT_TYPE:'Content', contentType: 'Plugin'}) return count(act) as count"
    val ASSET_SNAP_USED_ACTIVITIES = "match (cnt: domain {IL_FUNC_OBJECT_TYPE: 'Content'}) -[r: uses]-> (act: domain {IL_FUNC_OBJECT_TYPE:'Content', contentType: 'Plugin'}) WHERE lower(cnt.contentType) IN ['story', 'game', 'collection', 'worksheet'] return count(distinct act) as count"
    val ASSET_SNAP_TOTAL_TEMPLATES = "match (temp: domain {IL_FUNC_OBJECT_TYPE:'Content', contentType: 'Template'}) return count(temp) as count"
    val ASSET_SNAP_USED_TEMPLATES = "MATCH (temp: domain{IL_FUNC_OBJECT_TYPE:'Content', contentType: 'Template'}) - [r: associatedTo] - (cnc: domain{IL_FUNC_OBJECT_TYPE:'Concept'}) WHERE cnc.contentCount > 0 RETURN count(distinct temp) as count"
}