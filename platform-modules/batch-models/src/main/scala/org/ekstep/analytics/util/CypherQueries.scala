package org.ekstep.analytics.util

object CypherQueries {

    val CONTENT_SNAPSHOT_TOTAL_USER_COUNT = "MATCH (usr :User {type:'author'}) RETURN count(usr)"
    val CONTENT_SNAPSHOT_ACTIVE_USER_COUNT = "MATCH (usr:User {type:'author'})<-[r:createdBy]-(cnt: domain{IL_FUNC_OBJECT_TYPE:'Content'}) WHERE lower(cnt.contentType) IN ['story', 'game', 'collection', 'worksheet'] RETURN usr.IL_UNIQUE_ID, cnt.createdOn"
    val CONTENT_SNAPSHOT_TOTAL_CONTENT_COUNT = "MATCH (cnt: domain{IL_FUNC_OBJECT_TYPE:'Content'}) WHERE lower(cnt.contentType) IN ['story', 'game', 'collection', 'worksheet'] RETURN count(cnt)"
    val CONTENT_SNAPSHOT_REVIEW_CONTENT_COUNT = "MATCH (cnt: domain{IL_FUNC_OBJECT_TYPE:'Content'}) WHERE lower(cnt.contentType) IN ['story', 'game', 'collection', 'worksheet'] AND cnt.status IN ['Review'] RETURN count(cnt)"
    val CONTENT_SNAPSHOT_LIVE_CONTENT_COUNT = "MATCH (cnt: domain{IL_FUNC_OBJECT_TYPE:'Content'}) WHERE lower(cnt.contentType) IN ['story', 'game', 'collection', 'worksheet'] AND cnt.status IN ['Live'] RETURN count(cnt)"
    val CONTENT_SNAPSHOT_AUTHOR_TOTAL_CONTENT_COUNT = "MATCH (usr:User{type:'author'}) RETURN usr.IL_UNIQUE_ID, usr.contentCount"
    val CONTENT_SNAPSHOT_AUTHOR_LIVE_CONTENT_COUNT = "MATCH (usr:User{type:'author'}) RETURN usr.IL_UNIQUE_ID, usr.liveContentCount"
    val CONTENT_SNAPSHOT_AUTHOR_REVIEW_CONTENT_COUNT = "MATCH (usr:User {type:'author'})<-[r:createdBy]-(cnt: domain{IL_FUNC_OBJECT_TYPE:'Content'}) WHERE lower(cnt.contentType) IN ['story', 'game', 'collection', 'worksheet'] AND cnt.status IN ['Review'] WITH usr,count(cnt) as rcc RETURN usr.IL_UNIQUE_ID, rcc"

}