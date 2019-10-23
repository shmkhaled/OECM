import org.apache.jena.graph
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

class RetrieveURIs(sparkSession: SparkSession) {
  //  def getTripleURIs(sourceOntology: RDD[graph.Triple], recordedTranslations: RDD[(String, String)],translatedTriples: RDD[(String, String, String)]): RDD[(String, String, String)] ={
  //    var sourceSubjectsURIs: RDD[(String, String)] = sourceOntology.map(x=>(x.getSubject.getURI,x.getSubject.getLocalName)).distinct()//.cache()
  ////    println("Subject URIs")
  ////    sourceSubjectsURIs.foreach(println(_))
  //    var sourceObjectsURIs = sourceOntology.filter(x=>x.getObject.isURI).map(x=> (x.getObject.getURI,x.getObject.getLocalName)).distinct()//.cache()
  ////    println("Object URIs")
  ////    sourceObjectsURIs.foreach(println(_))
  //    var sourcePredicateURIs: RDD[(String, String)] = sourceOntology.map(x=>(x.getPredicate.getURI,x.getPredicate.getLocalName)).distinct()//.cache()
  //    //    sourcePredicateURIs.foreach(println(_))
  //
  ////        println("################# All recorded translations ################")
  ////      recordedTranslations.foreach(println(_))
  //    val p = new PreProcessing()
  ////    println("sourceTranslatedSubjectsWithURI")
  //    var sourceTranslatedSubjectsWithURI = recordedTranslations.keyBy(_._1).join(sourceSubjectsURIs.keyBy(_._2)).distinct().map(x=>(p.getURIWithoutLastString(x._2._2._1)+x._2._1._2,x._2._1._2)).distinct()//.cache()
  ////    sourceTranslatedSubjectsWithURI.foreach(println(_))
  //
  //    //    System.out.println(p.getURIWithoutLastString("http://purl.org/semsur/Publikation"))
  ////    println("################# translated triples with Subject URIs ################")
  //    var translatedTriplesWithSubjectURIs = translatedTriples.keyBy(_._1).join(sourceTranslatedSubjectsWithURI.keyBy(_._2)).map(x=>(x._2._2._1,x._2._1._2,x._2._1._3))//.cache()
  ////    translatedTriplesWithSubjectURIs.foreach(println(_))
  //
  ////    println("sourceTranslatedObjectsWithURI")
  //    var sourceTranslatedObjectsWithURI: RDD[(String, String)] = recordedTranslations.keyBy(_._1).join(sourceObjectsURIs.keyBy(_._2)).distinct().map(x=>(p.getURIWithoutLastString(x._2._2._1)+x._2._1._2,x._2._1._2)).distinct().union(sourceObjectsURIs.filter(x=>x._2 == "Class"))//.cache()
  ////    sourceTranslatedObjectsWithURI.foreach(println(_))
  //
  ////    println("################# translated triples with Subject and Objects URIs ################")
  //    var translatedTriplesWithObjectURIs = translatedTriplesWithSubjectURIs.keyBy(_._3).leftOuterJoin(sourceTranslatedObjectsWithURI.keyBy(_._2)).map{case(x)=> if (x._2._2.isEmpty)(x._2._1) else (x._2._1._1,x._2._1._2,x._2._2.last._1)}//.cache()
  ////    translatedTriplesWithObjectURIs.foreach(println(_))
  //
  //
  ////    println("Fully translated triples with All URIs")
  //    var sourceTranslatedPredicatesWithURI= translatedTriplesWithObjectURIs.keyBy(_._2).join(sourcePredicateURIs.keyBy(_._2)).distinct().map(x=>(x._2._1._1,x._2._2._1,x._2._1._3))
  ////    sourceTranslatedPredicatesWithURI.foreach(println(_))
  //    sourceTranslatedPredicatesWithURI
  //
  //  }
  def getTripleURIs(sourceOntology: RDD[graph.Triple],sourceClassesWithBestTranslation: RDD[(String, String, String)],relationsWithTranslation: RDD[(String, String, String)],triplesForEnrichment: RDD[(String, String, String)]) = {
    val sourceSubjectURIs = sourceOntology.map(x => (x.getSubject.getNameSpace, x.getSubject.getLocalName))
    //    println("sourceSubjectURIs")
    //    sourceSubjectURIs.foreach(println(_))
    val sourceObjectURIs = sourceOntology.filter(x => x.getObject.isURI).map(y => (y.getObject.getNameSpace, y.getObject.getLocalName))
    //    println("sourceObjectURIs")
    //    sourceObjectURIs.foreach(println(_))
    val AllSourceURIs: RDD[(String, String)] = sourceSubjectURIs.union(sourceObjectURIs).distinct(2)
//    println("All source ontology URIs")
//    AllSourceURIs.foreach(println(_))

    val ontologyElementsWithCodes: RDD[(String, String, String)] = sourceClassesWithBestTranslation.union(relationsWithTranslation)
    val ontologyElementsWithCodesAndURIs: RDD[(String, String, String, String)] = ontologyElementsWithCodes.keyBy(_._1).join(AllSourceURIs.keyBy(_._2))
      .map{case (code1,((code2, foreignLabel, englishLabel),(uri,code3)))=> (uri,code1,foreignLabel,englishLabel)}
    println("Ontology Elements with codes and URIs " +ontologyElementsWithCodesAndURIs.count()+" ontologyElementsWithCodes number is "+ontologyElementsWithCodes.count())
    ontologyElementsWithCodesAndURIs.foreach(println(_))

    val ontologyElementsWithCodesAndURIsBroadcasting =  sparkSession.sparkContext.broadcast(ontologyElementsWithCodesAndURIs.keyBy(_._4).collect().toMap)
    val triplesForEnrichmentWithURIs= triplesForEnrichment.map(x => if (ontologyElementsWithCodesAndURIsBroadcasting.value.contains(x._1) || ontologyElementsWithCodesAndURIsBroadcasting.value.contains(x._3)) (ontologyElementsWithCodesAndURIsBroadcasting.value(x._1)._1.concat(x._1),x._2,ontologyElementsWithCodesAndURIsBroadcasting.value(x._3)._1.concat(x._3)) else (ontologyElementsWithCodesAndURIsBroadcasting.value(x._1)._1.concat(x._1),x._2,x._3))
    println("Triples for enrichment with URIs")
    triplesForEnrichmentWithURIs.foreach(println(_))
//    val ontologyWithSubjectLabels: RDD[(Node, Node, Node)] = ontologyTriples.filter(x=>x.getPredicate.getLocalName != "label")
//      .map(x => if(labelBroadcasting.value.contains(x.getSubject)) (labelBroadcasting.value(x.getSubject).getObject, x.getPredicate, x.getObject) else (x.getSubject, x.getPredicate, x.getObject))
  }

}
