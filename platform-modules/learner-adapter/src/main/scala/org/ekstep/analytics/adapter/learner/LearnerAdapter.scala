package org.ekstep.analytics.adapter.learner

import com.websudos.phantom.connectors._
import org.ekstep.analytics.adapter.model.LearnerActivityDAO
import org.ekstep.analytics.adapter.model.LearnerProficiencyDAO

/**
 * @author Santhosh
 */

object Defaults {
    val connector = ContactPoint.local.keySpace("learner_db");
}

class LearnerDatabase(val keyspace: KeySpaceDef) extends com.websudos.phantom.db.DatabaseImpl(keyspace) {
    object LearnerSnapshot extends LearnerActivityDAO with keyspace.Connector
    object LearnerProficiency extends LearnerProficiencyDAO with keyspace.Connector
}

object LearnerAdapter extends LearnerDatabase(Defaults.connector)