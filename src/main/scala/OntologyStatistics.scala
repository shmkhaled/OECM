
import org.apache.jena.graph
import org.apache.spark.rdd.RDD

class OntologyStatistics {
  def GetStatistics (ontologyTriples: RDD[graph.Triple])={
    println("======================================")
    println("|       Ontology Statistics      |")
    println("======================================")
    println("Number of triples in the ontology = "+ontologyTriples.count())
    ontologyTriples.foreach(println(_))
//    var subject = ontologyTriples.map(_.getSubject).distinct()
//    println("First five subjects are:")
//    subject.take(5).foreach(println(_))
//    println("Number of classes: "+subject.count())
//    var classes = ontologyTriples.filter(x=> x.getPredicate.getLocalName == "label").distinct()
//    classes.foreach(println(_))

    val sObjectProperty = ontologyTriples.filter(q => q.getObject.isURI && q.getObject.getLocalName == "ObjectProperty").distinct()
    println("Number of object properties is "+sObjectProperty.count())
//    sObjectProperty.foreach(println(_))

    val sAnnotationProperty = ontologyTriples.filter(q => q.getObject.isURI && q.getObject.getLocalName == "AnnotationProperty").distinct()
    println("Number of annotation properties is "+sAnnotationProperty.count())
//    sAnnotationProperty.foreach(println(_))

    val sDatatypeProperty = ontologyTriples.filter(q => q.getObject.isURI && q.getObject.getLocalName == "DatatypeProperty").distinct()
    println("Number of Datatype properties is "+sDatatypeProperty.count())
//    sDatatypeProperty.foreach(println(_))

    val sClass = ontologyTriples.filter(q => q.getSubject.isURI && q.getObject.isURI && q.getObject.getLocalName == "Class").distinct()
    println("Number of classes is "+sClass.count())
//    sClass.foreach(println(_))

    val listOfPredicates = ontologyTriples.map(x => x.getPredicate.getLocalName).distinct()
    println("List of predicates in the ontology: ")
    listOfPredicates.foreach(println(_))
//    println("All predicates without URIs:")
//    val targetPredicatesWithoutURIs = ontologyTriples.map(_.getPredicate.getLocalName).distinct()
//    targetPredicatesWithoutURIs.foreach(println(_))
//
//    var triplesWithSubClassAndDisJoint = ontologyTriples.filter(x=>x.getPredicate.getLocalName == "subClassOf" || x.getPredicate.getLocalName == "disjointWith")
////    println("Ontology triples "+triplesWithSubClassAndDisJoint.count())
////    triplesWithSubClassAndDisJoint.foreach(println(_))

  }
}
