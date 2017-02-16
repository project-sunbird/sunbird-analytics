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
import org.ekstep.analytics.adapter.ContentAdapter
import org.ekstep.analytics.adapter.ContentModel
import org.ekstep.analytics.util.ContentUsageSummaryFact
import org.ekstep.analytics.transformer.ContentUsageTransformer
import org.ekstep.analytics.util.ContentPopularitySummaryFact2

case class ContentRecos(content_id: String, scores: List[(String, Double)]) extends AlgoOutput with Output
case class ContentContext(c1_ctv: ContentToVector, c2_ctv: ContentToVector) extends AlgoInput
case class BlacklistContents(config_key: String, config_value: List[String])
case class ContentFeatures(content_id: String, num_downloads: Long, avg_rating: Double, total_interactions: Long)
case class ContentFeatures_t(content_id: String, num_downloads: Double, avg_rating: Double, total_interactions: Double)

object EndOfContentRecommendationModel extends IBatchModelTemplate[Empty, ContentContext, ContentRecos, ContentRecos] with Serializable {

    implicit val className = "org.ekstep.analytics.model.EndOfContentRecommendationModel"
    override def name: String = "EndOfContentRecommendationModel"
    
    override def preProcess(data: RDD[Empty], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[ContentContext] = {

        val contentVectors = sc.cassandraTable[ContentToVector](Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_TO_VEC);
        val combinations = contentVectors.cartesian(contentVectors).filter { case (a, b) => a != b }
        combinations.map { x => ContentContext(x._1, x._2) }
    }

    override def algorithm(data: RDD[ContentContext], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[ContentRecos] = {

        val defaultContentModel = ContentModel("" , List(), "", List())
        val defaultContentFeatures = ContentFeatures_t("", 0.0, 0.0, 0.0)
        
        val method = config.getOrElse("method", "cosine").asInstanceOf[String]
        val norm = config.getOrElse("norm", "none").asInstanceOf[String]
        val weight = config.getOrElse("weight", 0.1).asInstanceOf[Double]
        val filterBlacklistedContents = config.getOrElse("filterBlacklistedContents", false).asInstanceOf[Boolean];
        val num_bins_downloads = config.getOrElse("num_bins_downloads", 10).asInstanceOf[Int];
        val num_bins_rating = config.getOrElse("num_bins_rating", 10).asInstanceOf[Int];
        val num_bins_interactions = config.getOrElse("num_bins_interactions", 10).asInstanceOf[Int];
        val sorting_order = config.getOrElse("sorting_order", List("rel.num_downloads", "rel.avg_rating", "eng.total_interactions", "simi.score")).asInstanceOf[List[String]];
        
        //Content Model
        val contentModel = ContentAdapter.getPublishedContentForRE().map { x => (x.id, x) }
        val cm = sc.parallelize(contentModel)
        val contentMap = contentModel.toMap;
        
        // Content Usage Summaries
        val contentUsageSummaries = sc.cassandraTable[ContentUsageSummaryFact](Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_USAGE_SUMMARY_FACT).where("d_period=? and d_tag = 'all'", 0).map { x => x }.cache();
        val cus = contentUsageSummaries.map{x => (x.d_content_id, x.m_total_interactions)}
        
        // Content Popularity Summaries
        val contentpopularitySummaries = sc.cassandraTable[ContentPopularitySummaryFact2](Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_POPULARITY_SUMMARY_FACT).where("d_period=? and d_tag = 'all'", 0).map { x => x }.cache();
        val cps = contentpopularitySummaries.map{x => (x.d_content_id, x.m_avg_rating)}
        
        // Content sideloading Summaries
        val contentSideloading = sc.cassandraTable[ContentSideloading](Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_SIDELOADING_SUMMARY)
        val css = contentSideloading.map{x => (x.content_id, x.num_downloads)}
        
        val features = cm.leftOuterJoin(css).leftOuterJoin(cps).leftOuterJoin(cus).map{x => ContentFeatures(x._1, x._2._1._1._2.getOrElse(0L), x._2._1._2.getOrElse(0.0), x._2._2.getOrElse(0L))}
        val features_t = ContentUsageTransformer.getBinningForEOC(features, num_bins_downloads, num_bins_rating, num_bins_interactions).map{x => (x.content_id, x)}.collect().toMap
        
        val scores = data.map { x => ((x.c1_ctv.contentId, x.c2_ctv.contentId), x) }.mapValues { x =>
            getContentSimilarity(x.c1_ctv, x.c2_ctv, method, norm, weight)
        }.groupBy(x => x._1._1).mapValues(f => f.map(x => (x._1._2, x._2)).toList.sortBy(y => y._2).reverse)

        val filtered_scores = scores.leftOuterJoin(cm).mapValues{ x =>
            val c1_subject = x._2.getOrElse(defaultContentModel).subject
            val c1_grade = x._2.getOrElse(defaultContentModel).gradeList
            val listF_sub = x._1.filter(f => c1_subject.exists { contentMap.get(f._1).getOrElse(defaultContentModel).subject.contains(_) })
            val listF_grade = listF_sub.filter(f => c1_grade.exists { contentMap.get(f._1).getOrElse(defaultContentModel).gradeList.contains(_) })
            val sorted_list = getSortedList(listF_grade, features_t, sorting_order)
            sorted_list;
        }
        
        val final_scores = if (filterBlacklistedContents) {
            val blacklistedContents = sc.cassandraTable[BlacklistContents](Constants.PLATFORM_KEY_SPACE_NAME, Constants.RECOMMENDATION_CONFIG)
            val contentsList = blacklistedContents.filter { x => x.config_key.equals("content_reco_blacklist") }
            if (!contentsList.isEmpty()) {
                val contents = contentsList.first().config_value
                filtered_scores.map { x =>
                    val filteredlist = x._2.filterNot(f => contents.contains(f._1))
                    (x._1, filteredlist)
                }
            } else filtered_scores
        } else filtered_scores

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
    
    def getSortedList(score: List[(String, Double)], features: Map[String, ContentFeatures_t], sorting_order: List[String]): List[(String, Double)] = {
        
        val ls = score.map{x =>
            (x._1, x._2, getSortedFeatures(x._1, x._2, features.get(x._1).get, sorting_order))
        }
        val sorted_ls = ls.sortBy(f => f._3).reverse.map(x => (x._1, x._2))
        sorted_ls;
    }
    
    def getSortedFeatures(id: String, simi_score: Double, features: ContentFeatures_t, sorting_order: List[String]): (Double, Double, Double, Double) = {
        
        val value1 = getValue(sorting_order(0), simi_score, features)
        val value2 = getValue(sorting_order(1), simi_score, features)
        val value3 = getValue(sorting_order(2), simi_score, features)
        val value4 = getValue(sorting_order(3), simi_score, features)
        (value1, value2, value3, value4);
    }
    
    def getValue(key: String, simi_score: Double, features: ContentFeatures_t): Double = {
        key match {
            case "rel.num_downloads" =>
                features.num_downloads;
            case "rel.avg_rating" =>
                features.avg_rating;
            case "eng.total_interactions" =>
                features.total_interactions;
            case "simi.score" =>
                simi_score;
            case _ =>
                throw new Exception("Unknown feature name found");
        }
    }
}