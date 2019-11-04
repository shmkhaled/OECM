import org.apache.spark.rdd.RDD

/*
* Created by Shimaa Ibrahim 4 November 2019
* */
class RelationSimilarity {
  def GetRelationSimilarity (listOfRelationsInTargetOntology: RDD[(String)], listOfRelationsInSourceOntology: RDD[String])={
    var crossRelations: RDD[(String, String)] = listOfRelationsInTargetOntology.cartesian(listOfRelationsInSourceOntology)
    val gS = new GetSimilarity()
    val p = new PreProcessing()

    var sim = crossRelations.map(x => (p.splitCamelCase(x._1).toLowerCase,x._2.toLowerCase, gS.getSimilarity(p.splitCamelCase(x._1).toLowerCase, x._2.toLowerCase))).filter(y=>y._3>0.5)
    println("Relation similarity: ")
    sim.foreach(println(_))
//    sim.coalesce(1, shuffle = true).saveAsTextFile("Output/RelationsSimilarity")
  }
  def Try (listOfRelationsInTargetOntology: RDD[(String)], translatedSourceOntologyRelationTriples: RDD[(String, String, String)])={
    val listOfRelationsInSourceOntology: RDD[String] = translatedSourceOntologyRelationTriples.map(x => x._1).distinct(2)
    var crossRelations: RDD[(String, String)] = listOfRelationsInTargetOntology.cartesian(listOfRelationsInSourceOntology)
    val gS = new GetSimilarity()
    val p = new PreProcessing()
    //    var sim: RDD[(String, String, Double)] = crossRelations.map(x=>( x._1.toLowerCase,x._2.toLowerCase,gS.getSimilarity(x._1,x._2))).filter(y=>y._3>0.9)

    var sim = crossRelations.map(x => (p.splitCamelCase(x._1).toLowerCase,x._2.toLowerCase, gS.getSimilarity(p.splitCamelCase(x._1).toLowerCase, x._2.toLowerCase))).filter(y=>y._3>0.9)
  }

}
