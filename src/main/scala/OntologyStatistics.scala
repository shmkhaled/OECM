
import net.sansa_stack.rdf.spark.model._
import org.apache.jena.graph
import org.apache.jena.graph.NodeFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.util.control.Exception.allCatch

class OntologyStatistics (sparkSession: SparkSession) {
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
  def RetrieveClasses (ontologyTriples: RDD[graph.Triple]): RDD[(String, String)]={
    val firstClass = ontologyTriples.find(None, None, Some(NodeFactory.createURI("http://www.w3.org/2002/07/owl#Class"))).first().getSubject.getLocalName
    var classes = sparkSession.sparkContext.emptyRDD[(String, String)]
    if (isNumber(firstClass))
      classes = RetrieveClassesWithURIsAndLabels (ontologyTriples: RDD[graph.Triple])
    else
      classes = RetrieveClassesWithLabels (ontologyTriples: RDD[graph.Triple]).zipWithIndex().map(x=>(x._1,x._2.toString))
    classes
  }
  def isNumber(s: String): Boolean = (allCatch opt s.toDouble).isDefined
  def isAllDigits(x: String): Boolean = x forall Character.isDigit
  def RetrieveClassesWithLabels (ontologyTriples: RDD[graph.Triple]): RDD[String]={ //will be applied for ontologies without codes like SEO
    val classesWithoutURIs: RDD[String] = ontologyTriples.find(None, None, Some(NodeFactory.createURI("http://www.w3.org/2002/07/owl#Class")))
      .map(x => x.getSubject.getLocalName)
    classesWithoutURIs
  }
  def RetrieveClassesWithURIsAndLabels (ontologyTriples: RDD[graph.Triple]): RDD[(String, String)]={ //will be applied for ontologies with codes like Multifarm ontologies
    var classes = ontologyTriples.find(None, None, Some(NodeFactory.createURI("http://www.w3.org/2002/07/owl#Class"))).keyBy(_.getSubject.getLocalName)
      .join(ontologyTriples.keyBy(_.getSubject.getLocalName))
      .filter(x=> x._2._2.getPredicate.getLocalName == "label")
      .map(y=> (y._1, y._2._2.getObject.getLiteral.getLexicalForm.split("@").head)).distinct()
    classes
  }
  def RetrieveClassesWithoutLabels (o: RDD[graph.Triple]): RDD[String]={
    val p = new PreProcessing()
    var classesWithoutURIs: RDD[String] = o.map(y=>p.stringPreProcessing(y.getSubject.getLocalName)).distinct().union(o.map{case(x)=> if(x.getObject.isURI)(p.stringPreProcessing(x.getObject.getLocalName))else null}.filter(y => y != null && y != "class")).distinct()//for classes with local names ex:ekaw-en, edas and SEO ontologies
    classesWithoutURIs
  }
}
