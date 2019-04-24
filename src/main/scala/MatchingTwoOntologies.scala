import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
/*
* Created by Shimaa 7.11.2018
* */
class MatchingTwoOntologies(sp: SparkSession) {
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
    tripelsForAdd = tripelsForAdd.keyBy(_._1).join(sourceSubOntology.keyBy(_._1)).map({case(a,((b,c,d,e),(s,p,o)))=> (s,p,o,'A')}).distinct()//.persist(StorageLevel.DISK_ONLY)
//    println("############### Triples to add ###############################")
//    tripelsForAdd.foreach(println(_))
//    println("###########################################################")

    tripelsForEnrichment = tripelsForEnrichment.union(tripelsForAdd)

    //.map({case(a,((s,p,o),b))=> if(!a.isEmpty()) (s,p,o,'E') else if (a.isEmpty()) (s,p,o,'A')})
    tripelsForEnrichment
  }

  def GetTriplesToBeEnriched2(sOntology: RDD[(String, String, String)], targetClassesWithoutURIs: RDD[String], listOfMatchedTerms: RDD[List[String]]): RDD[(String, String, String)]={
    var matchedTerms: RDD[String] = listOfMatchedTerms.map(x=>x(1))

    //Get all triples from the source ontology which has the matched terms as subject or object
    var triples0: RDD[(String, String, String)] = sOntology.keyBy(_._1).join(matchedTerms.zipWithIndex()).map({case(a,((s,p,o),b))=> (s,p,o)}).union(sOntology.keyBy(_._3).join(matchedTerms.zipWithIndex()).map({case(a,((s,p,o),b))=> (s,p,o)})).filter(x=>x._2!= "type").distinct()//.persist(StorageLevel.DISK_ONLY)
      //.union(sOntology.keyBy(_._3).join(matchedTerms.zipWithIndex())).distinct()
//    println("Triples 0:")
//    triples0.foreach(println(_))
    var sourceOntology: RDD[(String, String, String)] = sOntology.subtract(triples0)
    //    var triples1: RDD[(String, String, String)] = sOntology.keyBy(_._3).join(matchedTerms.zipWithIndex()).map({case(a,((s,p,o),b))=> (s,p,o)})
    //    println("Triples 1:")
    //    triples1.foreach(println(_))

    //List of target classes should be updated every time, by adding new classes, after getting triples to be enriched.
//    var listOfTargetClasses = targetClassesWithoutURIs.union(triples0.map(x=>x._1)).union(triples0.map(x=>x._3)).distinct()
    var listOfNewClasses = triples0.map(x=>x._1).union(triples0.map(x=>x._3)).distinct().subtract(matchedTerms)//.persist(StorageLevel.DISK_ONLY)
    listOfNewClasses.foreach(println(_))
    var i = 1
    var triples1 = triples0
    while (listOfNewClasses.count() != 0){
      println("Iteration number "+i)
      matchedTerms = matchedTerms.union(listOfNewClasses)
//      println("number of target classes " +matchedTerms.count())

//      triples0 = sOntology.keyBy(_._1).join(listOfNewClasses.zipWithIndex()).map({case(a,((s,p,o),b))=> (s,p,o)}).union(sOntology.keyBy(_._3).join(listOfNewClasses.zipWithIndex()).map({case(a,((s,p,o),b))=> (s,p,o)})).distinct().cache()
//        triples0 = sourceOntology.keyBy(_._1).join(listOfNewClasses.zipWithIndex()).map({case(a,((s,p,o),b))=> (s,p,o)}).union(sourceOntology.keyBy(_._3).join(listOfNewClasses.zipWithIndex()).map({case(a,((s,p,o),b))=> (s,p,o)})).distinct()//.persist(StorageLevel.DISK_ONLY)
      var rdd1 = sourceOntology.keyBy(_._1).join(listOfNewClasses.zipWithIndex()).map({case(a,((s,p,o),b))=> (s,p,o)})
      var rdd2 = sourceOntology.keyBy(_._3).join(listOfNewClasses.zipWithIndex()).map({case(a,((s,p,o),b))=> (s,p,o)})
      triples0 = rdd1.union(rdd2).distinct()

      listOfNewClasses = triples0.map(x=>x._1).union(triples0.map(x=>x._3)).distinct().subtract(matchedTerms).distinct().filter(x => x != "Class")//.persist(StorageLevel.DISK_ONLY)
//      println("New triples:")
//      triples0.foreach(println(_))
      sourceOntology = sOntology.subtract(triples0)
      triples1 = triples1.union(triples0).distinct()
//      println("number of new classes " +listOfNewClasses.count()) //try collect.size
//      println("New classes are:")
//      listOfNewClasses.foreach(println(_))
      i = i + 1
    }

    //    var triples1 = triples0.keyBy(_._1).join(sOntology.keyBy(_._1)).map{case(a,((c,d,e),(s,p,o))) => (s,p,o)}.distinct()
//    var triples1 = triples0.keyBy(_._1).join(sOntology.keyBy(_._1)).map{case(a,((c,d,e),(s,p,o))) => (s,p,o)}.distinct()
//    triples0 = sOntology
//    println("Triples 1:" + triples1.count())
//    triples1.foreach(println(_))

    var newClasses = matchedTerms.subtract(listOfMatchedTerms.map(x=>x(1)))
    println("List of new classes: "+newClasses.count())
    newClasses.foreach(println(_))
    triples1

  }
def GetTriplesToBeEnriched(sOntology: RDD[(String, String, String)], targetClassesWithoutURIs: RDD[String], listOfMatchedTerms: RDD[List[String]]): RDD[(String, String, String)]={
    val matchedTerms: RDD[String] = listOfMatchedTerms.map(x=>x(1))
    var classes = matchedTerms
    var triples = sp.sparkContext.emptyRDD[(String, String, String)]
    var sourceOntology: RDD[(String, String, String)] = sOntology
    var i = 1
    while (classes.count() != 0){
      println("Iteration number "+i)
      //Get all triples from the source ontology which has the matched terms as subject or object
      var tempTriples = sourceOntology.keyBy(_._1).join(classes.zipWithIndex()).map({case(a,((s,p,o),b))=> (s,p,o)}).union(sourceOntology.keyBy(_._3).join(classes.zipWithIndex()).map({case(a,((s,p,o),b))=> (s,p,o)})).distinct()
      if (i == 1)
        tempTriples = tempTriples.filter(x=>x._2!= "type")
      println("New triples:")
      tempTriples.foreach(println(_))
      sourceOntology = sourceOntology.subtract(tempTriples)
      classes = tempTriples.map(x=>x._1).union(tempTriples.map(x=>x._3)).subtract(classes).distinct().filter(x => x != "Class")
      println("New classes are:")
      classes.foreach(println(_))
      triples = triples.union(tempTriples)
      i = i + 1
    }

//    val newClasses = matchedTerms.subtract(listOfMatchedTerms.map(x=>x(1)))
//    println("List of new classes: "+newClasses.count())
//    newClasses.foreach(println(_))
    triples

  }

}
