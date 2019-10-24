import java.io

import org.apache.jena.graph
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

class RetrieveURIs(sparkSession: SparkSession, sourceOntology: RDD[graph.Triple]) {
  val sourceSubjectURIs = sourceOntology.map(x => (x.getSubject.getNameSpace, x.getSubject.getLocalName))

  val sourcePredicateURIs = sourceOntology.filter(x => x.getPredicate.isURI).map(y => (y.getPredicate.getNameSpace, y.getPredicate.getLocalName))

  val sourceObjectURIs = sourceOntology.filter(x => x.getObject.isURI).map(y => (y.getObject.getNameSpace, y.getObject.getLocalName))

  val AllSourceURIs: RDD[(String, String)] = sourceSubjectURIs.union(sourcePredicateURIs).union(sourceObjectURIs).distinct(2) //    println("All source ontology URIs")
  //    AllSourceURIs.foreach(println(_))
  //  def getTripleURIs(sourceOntology: RDD[graph.Triple], sourceClassesWithBestTranslation: RDD[(String, String, String)], relationsWithTranslation: RDD[(String, String, String)], triplesForEnrichment: RDD[(String, String, String)], triplesFor: String): RDD[(String, String, String)] = { //triplesFor is a flage which can take "classes" or "relations"
  //
  //    val ontologyElementsWithCodes: RDD[(String, String, String)] = sourceClassesWithBestTranslation.union(relationsWithTranslation)
  //
  //    val ontologyElementsWithCodesAndURIs: RDD[(String, io.Serializable, io.Serializable, String)] = ontologyElementsWithCodes.keyBy(_._1).rightOuterJoin(AllSourceURIs.keyBy(_._2)).map { case (code1, (Some((code2, foreignLabel, englishLabel)), (uri, code3))) => (uri, code1, foreignLabel, englishLabel)
  //    case (s, (None, (urii, o))) => (urii, None, None, o)
  //    }
  //
  //    //    println("Ontology Elements with codes and URIs " +ontologyElementsWithCodesAndURIs.count()+" ontologyElementsWithCodes number is "+ontologyElementsWithCodes.count())
  //    //
  //    //    ontologyElementsWithCodesAndURIs.foreach(println(_))
  //    val ontologyElementsWithCodesAndURIsBroadcasting = sparkSession.sparkContext.broadcast(ontologyElementsWithCodesAndURIs.keyBy(_._4).collect().toMap)
  //
  //    val prePross = new PreProcessing()
  //
  //    var triplesForEnrichmentWithURIs = sparkSession.sparkContext.emptyRDD[(String, String, String)]
  //    if (triplesFor == "classes") {
  //      triplesForEnrichmentWithURIs = triplesForEnrichment.map(x => if (ontologyElementsWithCodesAndURIsBroadcasting.value.contains(x._1) || ontologyElementsWithCodesAndURIsBroadcasting.value.contains(x._2) || ontologyElementsWithCodesAndURIsBroadcasting.value.contains(x._3)) (ontologyElementsWithCodesAndURIsBroadcasting.value(x._1)._1.concat(prePross.ToCamelForClass(x._1)), ontologyElementsWithCodesAndURIsBroadcasting.value(x._2)._1.concat(x._2), ontologyElementsWithCodesAndURIsBroadcasting.value(x._3)._1.concat(prePross.ToCamelForClass(x._3))) else (ontologyElementsWithCodesAndURIsBroadcasting.value(x._1)._1.concat(prePross.ToCamelForClass(x._1)), prePross.ToCamelForClass(x._2), prePross.ToCamelForClass(x._3)))
  //    }
  //    else
  //      triplesForEnrichmentWithURIs = triplesForEnrichment.map(x => if (ontologyElementsWithCodesAndURIsBroadcasting.value.contains(x._1) || ontologyElementsWithCodesAndURIsBroadcasting.value.contains(x._2) || ontologyElementsWithCodesAndURIsBroadcasting.value.contains(x._3)) (ontologyElementsWithCodesAndURIsBroadcasting.value(x._1)._1.concat(prePross.ToCamelForRelation(x._1)), ontologyElementsWithCodesAndURIsBroadcasting.value(x._2)._1.concat(x._2), ontologyElementsWithCodesAndURIsBroadcasting.value(x._3)._1.concat(prePross.ToCamelForClass(x._3))) else (ontologyElementsWithCodesAndURIsBroadcasting.value(x._1)._1.concat(prePross.ToCamelForRelation(x._1)), prePross.ToCamelForClass(x._2), prePross.ToCamelForClass(x._3)))
  //    triplesForEnrichmentWithURIs
  //  }
  def getTripleURIsForHierarchicalEnrichment(sourceClassesWithBestTranslation: RDD[(String, String, String)], triplesForEnrichment: RDD[(String, String, String)]): RDD[(String, String, String)] = {

    val ontologyClassesWithCodesAndURIs: RDD[(String, io.Serializable, io.Serializable, String)] = sourceClassesWithBestTranslation.keyBy(_._1).rightOuterJoin(AllSourceURIs.keyBy(_._2)).map { case (code1, (Some((code2, foreignLabel, englishLabel)), (uri, code3))) => (uri, code1, foreignLabel, englishLabel)
    case (s, (None, (urii, o))) => (urii, None, None, o)
    }

    val ontologyClassesWithCodesAndURIsBroadcasting = sparkSession.sparkContext.broadcast(ontologyClassesWithCodesAndURIs.keyBy(_._4).collect().toMap)

    val prePross = new PreProcessing()

    val triplesForEnrichmentWithURIs = triplesForEnrichment.map(x => if (ontologyClassesWithCodesAndURIsBroadcasting.value.contains(x._1) || ontologyClassesWithCodesAndURIsBroadcasting.value.contains(x._2) || ontologyClassesWithCodesAndURIsBroadcasting.value.contains(x._3)) (ontologyClassesWithCodesAndURIsBroadcasting.value(x._1)._1.concat(prePross.ToCamelForClass(x._1)), ontologyClassesWithCodesAndURIsBroadcasting.value(x._2)._1.concat(x._2), ontologyClassesWithCodesAndURIsBroadcasting.value(x._3)._1.concat(prePross.ToCamelForClass(x._3))) else (ontologyClassesWithCodesAndURIsBroadcasting.value(x._1)._1.concat(prePross.ToCamelForClass(x._1)), x._2, prePross.ToCamelForClass(x._3)))

    triplesForEnrichmentWithURIs
  }

  def getTripleURIsForRelationalEnrichment(relationsWithTranslation: RDD[(String, String, String)], sourceClassesWithBestTranslation: RDD[(String, String, String)], triplesForEnrichment: RDD[(String, String, String)]): RDD[(String, String, String)] = {

    val ontologyElementsWithCodes: RDD[(String, String, String)] = sourceClassesWithBestTranslation.union(relationsWithTranslation)

    val ontologyElementsWithCodesAndURIs: RDD[(String, io.Serializable, io.Serializable, String)] = ontologyElementsWithCodes.keyBy(_._1).rightOuterJoin(AllSourceURIs.keyBy(_._2)).map { case (code1, (Some((code2, foreignLabel, englishLabel)), (uri, code3))) => (uri, code1, foreignLabel, englishLabel)
    case (s, (None, (urii, o))) => (urii, None, None, o)
    }

    val ontologyElementsWithCodesAndURIsBroadcasting = sparkSession.sparkContext.broadcast(ontologyElementsWithCodesAndURIs.keyBy(_._4).collect().toMap)

    val prePross = new PreProcessing()

    val subjectCapitalization = triplesForEnrichment.map(x => if (ontologyElementsWithCodesAndURIsBroadcasting.value.contains(x._1) || ontologyElementsWithCodesAndURIsBroadcasting.value.contains(x._2) || ontologyElementsWithCodesAndURIsBroadcasting.value.contains(x._3)) (ontologyElementsWithCodesAndURIsBroadcasting.value(x._1)._1.concat(prePross.ToCamelForRelation(x._1)), ontologyElementsWithCodesAndURIsBroadcasting.value(x._2)._1.concat(x._2), ontologyElementsWithCodesAndURIsBroadcasting.value(x._3)._1.concat(x._3)) else (ontologyElementsWithCodesAndURIsBroadcasting.value(x._1)._1.concat(prePross.ToCamelForRelation(x._1)), x._2, x._3))

    val sourceClassesWithBestTranslationBroadcasting = sparkSession.sparkContext.broadcast(sourceClassesWithBestTranslation.keyBy(_._3).collect().toMap)

    val triplesForEnrichmentWithURIs = subjectCapitalization.map(x => if (sourceClassesWithBestTranslationBroadcasting.value.contains(x._3.split("#").last)) (x._1, x._2, x._3.split("#").head.concat("#" + prePross.ToCamelForClass(x._3.split("#").last))) else (x._1, x._2, x._3.split("#").head.concat("#" + prePross.ToCamelForRelation(x._3.split("#").last))))
    triplesForEnrichmentWithURIs
  }
}
