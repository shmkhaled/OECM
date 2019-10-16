
import org.apache.jena.graph
import org.apache.jena.graph.NodeFactory
import org.apache.spark.rdd.RDD
import net.sansa_stack.rdf.spark.model._

class OntologyStatistics {
  def GetStatistics (ontologyTriples: RDD[graph.Triple])={
    println("======================================")
    println("|       Ontology Statistics      |")
    println("======================================")
    val ontoName = ontologyTriples.filter(x => x.getPredicate.getLocalName == "type" && x.getObject.getLocalName == "Ontology")
      .map(x => x.getSubject.getLocalName).first()
    println("Ontology name is: "+ontoName)
//    ontoName.foreach(println(_))
    println("Number of triples in the "+ontoName+" ontology = "+ontologyTriples.count())
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
  def RetrieveClassesWithLabels (ontologyTriples: RDD[graph.Triple]): RDD[String]={
//    val classesWithoutURIs = ontologyTriples.filter(x=>x.getPredicate.getLocalName == "label" && x.getObject.getLocalName == "Class")
//      .map(y=>y.getObject.getLiteral.getLexicalForm.split("@").head).distinct()
    val z: RDD[String] = ontologyTriples.find(None, None, Some(NodeFactory.createURI("http://www.w3.org/2002/07/owl#Class")))
      .map(x => x.getSubject.getLocalName)
//    println("Classes are "+z.count())
//    z.foreach(println(_))
//    classesWithoutURIs
    z
  }
}
