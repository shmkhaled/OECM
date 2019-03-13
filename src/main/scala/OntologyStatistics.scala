
import org.apache.jena.graph
import org.apache.spark.rdd.RDD

class OntologyStatistics {
  def GetStatistics (ontologyTriples: RDD[graph.Triple])={
    println("======================================")
    println("|       Ontology Statistics      |")
    println("======================================")
    ontologyTriples.take(5).foreach(println(_))
    println("Number of triples in the ontology = "+ontologyTriples.count())
    var subject = ontologyTriples.map(_.getSubject).distinct()
//    println("First five subjects are:")
//    subject.take(5).foreach(println(_))
//    println("Number of classes: "+subject.count())
//    var classes = ontologyTriples.filter(x=> x.getPredicate.getLocalName == "label").distinct()
//    classes.foreach(println(_))
//
    println("All predicates without URIs:")
    val targetPredicatesWithoutURIs = ontologyTriples.map(_.getPredicate.getLocalName).distinct()
    targetPredicatesWithoutURIs.foreach(println(_))

    var triplesWithSubClassAndDisJoint = ontologyTriples.filter(x=>x.getPredicate.getLocalName == "subClassOf" || x.getPredicate.getLocalName == "disjointWith")
//    println("Ontology triples "+triplesWithSubClassAndDisJoint.count())
//    triplesWithSubClassAndDisJoint.foreach(println(_))

  }
}
