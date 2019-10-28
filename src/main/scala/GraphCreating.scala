import org.apache.jena.graph
import org.apache.jena.graph.NodeFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

class GraphCreating(sparkSession: SparkSession) extends Serializable {
  val prePross = new PreProcessing()
  def createGraph (triplesForEnrichmentWithURIs: RDD[(String, String, String)]): RDD[graph.Triple]={
    val g1: RDD[graph.Triple] = triplesForEnrichmentWithURIs.map{case x => graph.Triple.create(
      NodeFactory.createURI(x._1),
      NodeFactory.createURI(x._2),
      NodeFactory.createURI(x._3)
    )}
    val listOfNewClassesWithURIs = triplesForEnrichmentWithURIs.map(x=> x._1).distinct(2)
//    println("listOfNewClassesWithURIs"+listOfNewClassesWithURIs.count())
//    listOfNewClassesWithURIs.foreach(println(_))
    val g2 = listOfNewClassesWithURIs.map{case x => graph.Triple.create(
      NodeFactory.createURI(x),
      NodeFactory.createURI("http://www.w3.org/2000/01/rdf-schema#label"),
      NodeFactory.createLiteral(prePross.splitCamelCase(x.split("#").last), "en")
    )}
    val g = g1.union(g2)
    g
  }
  def createMultilingualGraphLabelsForClasses(triplesForEnrichmentWithURIs: RDD[(String, String, String)], sourceClassesWithBestTranslation: RDD[(String, String, String)]): RDD[graph.Triple]={
    val sourceClassesWithBestTranslationBroadcasting = sparkSession.sparkContext.broadcast(sourceClassesWithBestTranslation.keyBy(_._3).collect().toMap)

    val triplesWithTranslation: RDD[(String, String, String)] = triplesForEnrichmentWithURIs.map(x =>
      if (sourceClassesWithBestTranslationBroadcasting.value.contains(prePross.splitCamelCase(x._1.split("#").last).toLowerCase)) (x._1,"http://www.w3.org/2000/01/rdf-schema#label",prePross.ToCamelForClass(sourceClassesWithBestTranslationBroadcasting.value(prePross.splitCamelCase(x._1.split("#").last).toLowerCase)._2))
      else (x._1, x._2,x._3))

    val g: RDD[graph.Triple] = triplesWithTranslation.map{case x => graph.Triple.create(
      NodeFactory.createURI(x._1),
      NodeFactory.createURI(x._2),
      NodeFactory.createLiteral(prePross.splitCamelCase(x._3), "de")
    )}
    g
  }
  def createMultilingualGraphLabelsForRelations(triplesForEnrichmentWithURIs: RDD[(String, String, String)], relationsWithTranslation: RDD[(String, String, String)]): RDD[graph.Triple]={
    val relationsWithTranslationBroadcasting = sparkSession.sparkContext.broadcast(relationsWithTranslation.keyBy(_._3).collect().toMap)

    val triplesWithTranslation: RDD[(String, String, String)] = triplesForEnrichmentWithURIs.map(x =>
      if (relationsWithTranslationBroadcasting.value.contains(prePross.splitCamelCase(x._1.split("#").last).toLowerCase)) (x._1,"http://www.w3.org/2000/01/rdf-schema#label",prePross.ToCamelForRelation(relationsWithTranslationBroadcasting.value(prePross.splitCamelCase(x._1.split("#").last).toLowerCase)._2))
      else (x._1, x._2,x._3))

    val relationsWithLiterals: RDD[(String, String, String)] = triplesWithTranslation.filter(x => x._2 == "http://www.w3.org/2000/01/rdf-schema#label")

    val g: RDD[graph.Triple] = relationsWithLiterals.map{case x => graph.Triple.create(
      NodeFactory.createURI(x._1),
      NodeFactory.createURI(x._2),
      NodeFactory.createLiteral(prePross.splitCamelCase(x._3), "de")
    )}
    g
  }

}
