
import net.sansa_stack.rdf.spark.model._
import org.apache.jena.graph
import org.apache.jena.graph.NodeFactory
import org.apache.spark.rdd.RDD

/*
* Created by Shimaa 15.10.2018
* */

class SubgraphExtraction{
  def extract(preProcessedSubSourceOntology: RDD[(String, String, String)], predefinedProperties: Array[String]):
  RDD[(String, String, String)] = {
    var uPredicate1: String = predefinedProperties.apply(0)
    var uPredicate2: String = ""
    if (predefinedProperties.length == 2) {
      uPredicate2 = predefinedProperties.apply(1)
    }
    val sourceSubOntology = preProcessedSubSourceOntology.filter(x=>(x._2 == uPredicate1 || x._2 == uPredicate2))
    sourceSubOntology

  }
  def extractFromRDD_Graph(sourceOntology: RDD[graph.Triple], predefinedProperties: Array[String]):
  RDD[graph.Triple] = {
    var uPredicate1: String = predefinedProperties.apply(0)
    var uPredicate2: String = ""
    if (predefinedProperties.length == 2) {
      uPredicate2 = predefinedProperties.apply(1)
    }
    val sourceSubOntology1: RDD[graph.Triple] = sourceOntology.find(None, Some(NodeFactory
      .createURI(s"http://www.w3.org/2000/01/rdf-schema#$uPredicate1")), None) //subClassOf
//      .createURI(s"http://www.w3.org/2000/01/rdf-schema#$uPredicate1")), None)
//    sourceSubOntology1.take(5).foreach(println(_))

    val sourceSubOntology2: RDD[graph.Triple] = sourceOntology.find(None, Some(NodeFactory
      .createURI(s"http://www.w3.org/2002/07/owl#$uPredicate2")), None) //disjointWith
//      .createURI(s"http://purl.org/semsur/$uPredicate2")), None)
//    sourceSubOntology2.take(5).foreach(println(_))

//    println("The extracted source ontologyTriples is:")
    val sourceSubOntology: RDD[graph.Triple] = sourceSubOntology1.union(sourceSubOntology2)
    sourceSubOntology

  }

}
