package org.ekstep.analytics.model

import org.ekstep.analytics.framework.IBatchModelTemplate
import org.ekstep.analytics.framework.Empty
import org.ekstep.analytics.framework.AlgoOutput
import org.ekstep.analytics.framework.Output
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.ekstep.analytics.framework.AlgoInput
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._
import org.ekstep.analytics.util.Constants
import org.ekstep.analytics.framework.util.CommonUtil
import breeze.linalg.DenseVector
import breeze.numerics._
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.framework.Level._
import org.ekstep.analytics.framework.ContentId

case class ContentRecos(content_id: String, scores: List[(String, Double)]) extends AlgoOutput with Output
case class ContentContext(c1_ctv: ContentToVector, c2_ctv: ContentToVector) extends AlgoInput
case class BlacklistContents(config_key: String, config_value: List[String])

object EndOfContentRecommendationModel extends IBatchModelTemplate[Empty, ContentContext, ContentRecos, ContentRecos] with Serializable {

    implicit val className = "org.ekstep.analytics.model.EndOfContentRecommendationModel"
    override def name: String = "EndOfContentRecommendationModel"
    
    override def preProcess(data: RDD[Empty], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[ContentContext] = {

        val contentVectors = sc.cassandraTable[ContentToVector](Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_TO_VEC);
        val combinations = contentVectors.cartesian(contentVectors).filter { case (a, b) => a != b }
        combinations.map { x => ContentContext(x._1, x._2) }
    }

    override def algorithm(data: RDD[ContentContext], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[ContentRecos] = {

        val method = config.getOrElse("method", "cosine").asInstanceOf[String]
        val norm = config.getOrElse("norm", "none").asInstanceOf[String]
        val weight = config.getOrElse("weight", 0.1).asInstanceOf[Double]
        val filterBlacklistedContents = config.getOrElse("filterBlacklistedContents", false).asInstanceOf[Boolean];

        val scores = data.map { x => ((x.c1_ctv.contentId, x.c2_ctv.contentId), x) }.mapValues { x =>
            getContentSimilarity(x.c1_ctv, x.c2_ctv, method, norm, weight)
        }.groupBy(x => x._1._1).mapValues(f => f.map(x => (x._1._2, x._2)).toList.sortBy(y => y._2).reverse)

        val final_scores = if (filterBlacklistedContents) {
            val blacklistedContents = sc.cassandraTable[BlacklistContents](Constants.PLATFORM_KEY_SPACE_NAME, Constants.RECOMMENDATION_CONFIG) //.where("config_key=?", "content_reco_blacklist").first.config_value
            val contentsList = blacklistedContents.filter { x => x.config_key.equals("content_reco_blacklist") } //.first().config_value
            if (!contentsList.isEmpty()) {
                val contents = contentsList.first().config_value
                scores.map { x =>
                    val filteredlist = x._2.filterNot(f => contents.contains(f._1))
                    (x._1, filteredlist)
                }
            } else scores
        } else scores

        final_scores.map { x =>
            ContentRecos(x._1, x._2)
        }
    }

    override def postProcess(data: RDD[ContentRecos], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[ContentRecos] = {

        data.saveToCassandra(Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_RECOS)
        data;
    }

    def getContentSimilarity(c1: ContentToVector, c2: ContentToVector, method: String, norm: String, weight: Double): Double = {

        val c12_text = computeSimilarity(c1.text_vec.get, c2.text_vec.get, method, norm)
        val c12_tag = computeSimilarity(c1.tag_vec.get, c2.tag_vec.get, method, norm)
        (weight * c12_text) + ((1 - weight) * c12_tag)
    }

    def computeSimilarity(c1: List[Double], c2: List[Double], method: String, norm: String): Double = {

        method match {
            case "cosine" =>
                cosineSimilarity(c1, c2)
            case _ =>
                throw new Exception("Unknown method found");
        }
    }

    def cosineSimilarity(x: List[Double], y: List[Double]): Double = {

        val vec1 = new DenseVector(x.toArray).t
        val vec2 = new DenseVector(y.toArray)
        vec1 * vec2
    }

}