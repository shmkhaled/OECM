import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
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
def GetTriplesToBeEnriched(sOntology: RDD[(String, String, String)], targetClassesWithoutURIs: RDD[String], listOfMatchedTerms: RDD[List[String]]): RDD[(String, String, String)]={
    val matchedTerms: RDD[String] = listOfMatchedTerms.map(x=>x(1))
    var newClasses: RDD[String] = matchedTerms
    var triples = sp.sparkContext.emptyRDD[(String, String, String)]
    var sourceOntology: RDD[(String, String, String)] = sOntology
    var i = 1
    while (newClasses.count() != 0){
      val startTimeMillis = System.currentTimeMillis()
      println("Iteration number "+i)
      //Get all triples from the source ontology which has the matched terms as subject or object
      var rdd1 = sourceOntology.keyBy(_._1).join(newClasses.zipWithIndex())
      var rdd11 = rdd1.map({case(a,((s,p,o),b))=> (s,p,o)})
      var rdd2 = sourceOntology.keyBy(_._3).join(newClasses.zipWithIndex())
      var rdd22 = rdd2.map({case(a,((s,p,o),b))=> (s,p,o)})
      var tempTriples = rdd11.union(rdd22).distinct(2)
      if (i == 1)
        tempTriples = tempTriples.filter(x=>x._2!= "type")
//      println("New triples:")
//      tempTriples.foreach(println(_))
      sourceOntology = sourceOntology.subtract(tempTriples)
      var subjectClass = tempTriples.map(x=>x._1)
      var objectClass = tempTriples.map(x=>x._3)
      var allClasses = subjectClass.union(objectClass).persist(StorageLevel.MEMORY_AND_DISK)
      newClasses = allClasses.subtract(newClasses).distinct(2).filter(x => x != "Class").persist(StorageLevel.MEMORY_AND_DISK)
      println("New classes are:")
      newClasses.foreach(println(_))
      triples = triples.union(tempTriples)
      val endTimeMillis = System.currentTimeMillis()
      val durationMilliSeconds = (endTimeMillis - startTimeMillis)
      println("runtime of iteration number "+i+" = "+durationMilliSeconds+ " ms")
      i = i + 1
    }

//    val newClasses = matchedTerms.subtract(listOfMatchedTerms.map(x=>x(1)))
//    println("List of new classes: "+newClasses.count())
//    newClasses.foreach(println(_))
    triples

  }


}
