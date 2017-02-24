package org.ekstep.analytics.vidyavaani.job

import org.ekstep.analytics.model.SparkSpec

class TestContentAssetRelationModel extends SparkSpec(null) {
  
    it should "create relation between asset & content" in {
        ContentAssetRelationModel.main("{}")(Option(sc));
    }
}