import org.apache.spark.rdd.RDD
/*
* Created by Shimaa 7.11.2018
* */
class MatchingTwoOntologies {
  def Match(sourceSubOntology: RDD[(String, String, String)], targetOntology: RDD[(String, String, String)], targetClassesWithoutURIs: RDD[String]): RDD[(String, String, String, Char)] ={
    /*
    * divide the source ontology into two sub-ontologies, the first one with all triples that have pridicate 'type', i.e. classes. The second one has the rest*/
    var matchOntology: RDD[(String, String, String)] = sourceSubOntology.intersection(targetOntology)
//      matchOntology.foreach(println(_))
//    println("|        The source ontologyTriples after removing the common triples       |")
    var sourceOntology: RDD[(String, String, String)] = sourceSubOntology.subtract(matchOntology).filter(x=>x._2 != "type")//remove full matched triples from the source ontologyTriples
//    sourceOntology.foreach(println(_))

    var tripelsForEnrichment: RDD[(String, String, String, Char)] = sourceOntology.keyBy(_._1).join(targetClassesWithoutURIs.zipWithIndex().keyBy(_._1)).map({case(a,((s,p,o),b))=> (s,p,o,'E')})
//    println("############### Triples to enrich ###############################")
//    tripelsForEnrichment.foreach(println(_))
//    println("###########################################################")
    var tripelsForAdd = tripelsForEnrichment.keyBy(_._3).join(sourceSubOntology.keyBy(_._1)).map({case(a,((b,c,d,e),(s,p,o)))=> (s,p,o,'A')}).union(sourceOntology.keyBy(_._3).join(targetClassesWithoutURIs.zipWithIndex().keyBy(_._1)).map({case(a,((s,p,o),b))=> (s,p,o,'A')})).distinct().cache()
    tripelsForAdd = tripelsForAdd.keyBy(_._1).join(sourceSubOntology.keyBy(_._1)).map({case(a,((b,c,d,e),(s,p,o)))=> (s,p,o,'A')}).distinct().cache()
//    println("############### Triples to add ###############################")
//    tripelsForAdd.foreach(println(_))
//    println("###########################################################")

    tripelsForEnrichment = tripelsForEnrichment.union(tripelsForAdd)

    //.map({case(a,((s,p,o),b))=> if(!a.isEmpty()) (s,p,o,'E') else if (a.isEmpty()) (s,p,o,'A')})
    tripelsForEnrichment
  }
  def GetTriplesToBeEnriched(sourceSubOntology: RDD[(String, String, String)], targetOntology: RDD[(String, String, String)], targetClassesWithoutURIs: RDD[String], listOfMatchedTerms: RDD[List[String]]): RDD[(String, String, String)]={
    var matchedTerms: RDD[String] = listOfMatchedTerms.map(x=>x(1))
    //Get all triples from the source ontology which has the matched terms as subject or object
    var triples0: RDD[(String, String, String)] = sourceSubOntology.keyBy(_._1).join(matchedTerms.zipWithIndex()).map({case(a,((s,p,o),b))=> (s,p,o)}).union(sourceSubOntology.keyBy(_._3).join(matchedTerms.zipWithIndex()).map({case(a,((s,p,o),b))=> (s,p,o)})).filter(x=>x._2!= "type").distinct().cache()
      //.union(sourceSubOntology.keyBy(_._3).join(matchedTerms.zipWithIndex())).distinct()
//    println("Triples 0:")
//    triples0.foreach(println(_))
    //    var triples1: RDD[(String, String, String)] = sourceSubOntology.keyBy(_._3).join(matchedTerms.zipWithIndex()).map({case(a,((s,p,o),b))=> (s,p,o)})
    //    println("Triples 1:")
    //    triples1.foreach(println(_))

    //List of target classes should be updated every time, by adding new classes, after getting triples to be enriched.
    var listOfTargetClasses = targetClassesWithoutURIs.union(triples0.map(x=>x._1)).union(triples0.map(x=>x._3)).distinct()
    var listOfNewClasses = triples0.map(x=>x._1).union(triples0.map(x=>x._3)).distinct().subtract(matchedTerms).cache()
    listOfNewClasses.foreach(println(_))
    var i = 1
    var triples1 = triples0
    while (listOfNewClasses.count() != 0){
      println("Iteration number "+i)
      matchedTerms = matchedTerms.union(listOfNewClasses)
      println("number of target classes " +matchedTerms.count())

      triples0 = sourceSubOntology.keyBy(_._1).join(listOfNewClasses.zipWithIndex()).map({case(a,((s,p,o),b))=> (s,p,o)}).union(sourceSubOntology.keyBy(_._3).join(listOfNewClasses.zipWithIndex()).map({case(a,((s,p,o),b))=> (s,p,o)})).distinct().cache()
      listOfNewClasses = triples0.map(x=>x._1).union(triples0.map(x=>x._3)).distinct().subtract(matchedTerms).distinct().filter(x => x != "Class").cache()
      triples1 = triples1.union(triples0).distinct()
      println("number of new classes " +listOfNewClasses.count())
      i = i + 1

    }

    //    var triples1 = triples0.keyBy(_._1).join(sourceSubOntology.keyBy(_._1)).map{case(a,((c,d,e),(s,p,o))) => (s,p,o)}.distinct()
//    var triples1 = triples0.keyBy(_._1).join(sourceSubOntology.keyBy(_._1)).map{case(a,((c,d,e),(s,p,o))) => (s,p,o)}.distinct()
//    triples0 = sourceSubOntology
//    println("Triples 1:" + triples1.count())
//    triples1.foreach(println(_))

    var newClasses = matchedTerms.subtract(listOfMatchedTerms.map(x=>x(1)))
    println("List of new classes: "+newClasses.count())
    newClasses.foreach(println(_))
    triples1

  }

}
