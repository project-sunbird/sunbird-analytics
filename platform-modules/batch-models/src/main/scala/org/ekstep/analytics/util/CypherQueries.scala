package org.ekstep.analytics.util

object CypherQueries {

    
    /**
     * Content Snapshot Summarizer Cypher Query
     * */
    // For author = partner = all
    val CONTENT_SNAPSHOT_TOTAL_USER_COUNT = "MATCH (usr :User {type:'author'}) RETURN count(usr)"
    val CONTENT_SNAPSHOT_ACTIVE_USER_COUNT = "MATCH (usr:User {type:'author'})<-[r:createdBy]-(cnt: domain{IL_FUNC_OBJECT_TYPE:'Content'}) WHERE lower(cnt.contentType) IN ['story', 'game', 'collection', 'worksheet'] RETURN usr.IL_UNIQUE_ID, cnt.createdOn"
    val CONTENT_SNAPSHOT_TOTAL_CONTENT_COUNT = "MATCH (cnt: domain{IL_FUNC_OBJECT_TYPE:'Content'}) WHERE lower(cnt.contentType) IN ['story', 'game', 'collection', 'worksheet'] RETURN count(cnt)"
    val CONTENT_SNAPSHOT_REVIEW_CONTENT_COUNT = "MATCH (cnt: domain{IL_FUNC_OBJECT_TYPE:'Content'}) WHERE lower(cnt.contentType) IN ['story', 'game', 'collection', 'worksheet'] AND cnt.status IN ['Review'] RETURN count(cnt)"
    val CONTENT_SNAPSHOT_LIVE_CONTENT_COUNT = "MATCH (cnt: domain{IL_FUNC_OBJECT_TYPE:'Content'}) WHERE lower(cnt.contentType) IN ['story', 'game', 'collection', 'worksheet'] AND cnt.status IN ['Live'] RETURN count(cnt)"
    // For specific author and partner = all
    val CONTENT_SNAPSHOT_AUTHOR_TOTAL_CONTENT_COUNT = "MATCH (usr:User{type:'author'}) RETURN usr.IL_UNIQUE_ID, usr.contentCount"
    val CONTENT_SNAPSHOT_AUTHOR_LIVE_CONTENT_COUNT = "MATCH (usr:User{type:'author'}) RETURN usr.IL_UNIQUE_ID, usr.liveContentCount"
    val CONTENT_SNAPSHOT_AUTHOR_REVIEW_CONTENT_COUNT = "MATCH (usr:User {type:'author'})<-[r:createdBy]-(cnt: domain{IL_FUNC_OBJECT_TYPE:'Content'}) WHERE lower(cnt.contentType) IN ['story', 'game', 'collection', 'worksheet'] AND cnt.status IN ['Review'] WITH usr,count(cnt) as rcc RETURN usr.IL_UNIQUE_ID, rcc"
    //For specific partner and author = all
    val CONTENT_SNAPSHOT_PARTNER_USER_COUNT = "MATCH (usr:User {type:'author'})<-[r:createdBy]-(cnt: domain{IL_FUNC_OBJECT_TYPE:'Content'}) WHERE lower(cnt.contentType) IN ['story', 'game', 'collection', 'worksheet'] AND EXISTS(cnt.createdFor) RETURN usr.IL_UNIQUE_ID, cnt.createdFor, cnt.createdOn"
    val CONTENT_SNAPSHOT_PARTNER_TOTAL_CONTENT_COUNT = "MATCH (cnt: domain{IL_FUNC_OBJECT_TYPE:'Content'}) WHERE lower(cnt.contentType) IN ['story', 'game', 'collection', 'worksheet'] AND EXISTS(cnt.createdFor) RETURN cnt.IL_UNIQUE_ID, cnt.createdFor"
    val CONTENT_SNAPSHOT_PARTNER_LIVE_CONTENT_COUNT = "MATCH (cnt: domain{IL_FUNC_OBJECT_TYPE:'Content'}) WHERE lower(cnt.contentType) IN ['story', 'game', 'collection', 'worksheet'] AND cnt.status IN ['Live'] AND EXISTS(cnt.createdFor) RETURN cnt.IL_UNIQUE_ID, cnt.createdFor"
    val CONTENT_SNAPSHOT_PARTNER_REVIEW_CONTENT_COUNT = "MATCH (cnt: domain{IL_FUNC_OBJECT_TYPE:'Content'}) WHERE lower(cnt.contentType) IN ['story', 'game', 'collection', 'worksheet'] AND cnt.status IN ['Review'] AND EXISTS(cnt.createdFor) RETURN cnt.IL_UNIQUE_ID, cnt.createdFor"
    
    /**
     * Concept Snapshot Summarizer Cypher Query
     * */
    
    val CONCEPT_SNAPSHOT_TOTAL_CONTENT_COUNT = "MATCH (cnc:domain{IL_FUNC_OBJECT_TYPE:'Concept'}) RETURN cnc.IL_UNIQUE_ID, cnc.contentCount"
    val CONCEPT_SNAPSHOT_REVIEW_CONTENT_COUNT = "MATCH (cnt:domain{IL_FUNC_OBJECT_TYPE:'Content'})-[r:associatedTo]->(cnc:domain{IL_FUNC_OBJECT_TYPE:'Concept'}) WHERE lower(cnt.contentType) IN ['story', 'game', 'collection', 'worksheet'] AND cnt.status IN ['Review'] WITH cnc, count(r) AS rcc RETURN cnc.IL_UNIQUE_ID, rcc"
    val CONCEPT_SNAPSHOT_LIVE_CONTENT_COUNT = "MATCH (cnc:domain{IL_FUNC_OBJECT_TYPE:'Concept'}) RETURN cnc.IL_UNIQUE_ID, cnc.liveContentCount"
    
   
    /**
     * Asset Snapshot Summarizer Cypher Query
     * 
     * */
    
    val ASSET_SNAP_MEDIA = "match (img: domain {IL_FUNC_OBJECT_TYPE:'Content', contentType: 'Asset'}) return img.mediaType"
    val ASSET_SNAP_USED_IMAGE = "match (img: domain {IL_FUNC_OBJECT_TYPE: 'Content', contentType: 'Asset', mediaType: 'image'}) <-[r: uses]- (cnt: domain {IL_FUNC_OBJECT_TYPE: 'Content'}) WHERE lower(cnt.contentType) IN ['story', 'game', 'collection', 'worksheet'] return count(distinct img)"
    val ASSET_SNAP_USED_AUDIO = "match (aud: domain {IL_FUNC_OBJECT_TYPE: 'Content', contentType: 'Asset', mediaType: 'audio'}) <-[r: uses]- (cnt: domain {IL_FUNC_OBJECT_TYPE: 'Content'}) WHERE lower(cnt.contentType) IN ['story', 'game', 'collection', 'worksheet'] return count(distinct aud)"
    val ASSET_SNAP_TOTAL_QUESTION = "match (as: domain {IL_FUNC_OBJECT_TYPE:'AssessmentItem'}) return count(as)"
    val ASSET_SNAP_USED_QUESTION = "MATCH (cnt: domain{IL_FUNC_OBJECT_TYPE:'Content'}) - [r1: associatedTo] -> (is: domain{IL_FUNC_OBJECT_TYPE:'ItemSet'}) - [r2: hasMember] -> (as: domain{IL_FUNC_OBJECT_TYPE:'AssessmentItem'}) WHERE lower(cnt.contentType) IN ['story', 'game', 'collection', 'worksheet'] RETURN count(distinct as)"
    val ASSET_SNAP_TOTAL_ACTIVITIES = "match (act: domain {IL_FUNC_OBJECT_TYPE:'Content', contentType: 'Plugin'}) return count(act)"
    val ASSET_SNAP_USED_ACTIVITIES = "match (cnt: domain {IL_FUNC_OBJECT_TYPE: 'Content'}) -[r: uses]-> (act: domain {IL_FUNC_OBJECT_TYPE:'Content', contentType: 'Plugin'}) WHERE lower(cnt.contentType) IN ['story', 'game', 'collection', 'worksheet'] return count(distinct act)"
    val ASSET_SNAP_TOTAL_TEMPLATES = "match (temp: domain {IL_FUNC_OBJECT_TYPE:'Content', contentType: 'Template'}) return count(temp)"
    val ASSET_SNAP_USED_TEMPLATES = "MATCH (temp: domain{IL_FUNC_OBJECT_TYPE:'Content', contentType: 'Template'}) - [r: associatedTo] - (cnc: domain{IL_FUNC_OBJECT_TYPE:'Concept'}) WHERE cnc.contentCount > 0 RETURN count(distinct temp)"
}