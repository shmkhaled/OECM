
import net.sansa_stack.rdf.spark.model._
import org.apache.jena.graph
import org.apache.jena.graph.{Node, NodeFactory}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

class OntologyStatistics (sparkSession: SparkSession) {
  def GetStatistics (ontologyTriples: RDD[graph.Triple])={
    println("======================================")
    println("|       Ontology Statistics      |")
    println("======================================")
    val ontoName = ontologyTriples.filter(x => x.getPredicate.getLocalName == "type" && x.getObject.getLocalName == "Ontology")
      .map(x => x.getSubject.getLocalName).first()
    println("Ontology name is: "+ontoName)
    println("Number of triples in the "+ontoName+" ontology = "+ontologyTriples.count())
//    triples.foreach(println(_))

    val sObjectProperty = ontologyTriples.filter(q => q.getObject.isURI && q.getObject.getLocalName == "ObjectProperty").distinct(2)
    println("Number of object properties is "+sObjectProperty.count())
//    sObjectProperty.foreach(println(_))

    val sAnnotationProperty = ontologyTriples.filter(q => q.getObject.isURI && q.getObject.getLocalName == "AnnotationProperty").distinct(2)
    println("Number of annotation properties is "+sAnnotationProperty.count())
//    sAnnotationProperty.foreach(println(_))

    val sDatatypeProperty = ontologyTriples.filter(q => q.getObject.isURI && q.getObject.getLocalName == "DatatypeProperty").distinct(2)
    println("Number of Datatype properties is "+sDatatypeProperty.count())
//    sDatatypeProperty.foreach(println(_))

    val sClass = ontologyTriples.filter(q => q.getSubject.isURI && q.getObject.isURI && q.getObject.getLocalName == "Class").distinct(2)
    println("Number of classes is "+sClass.count())
//    sClass.foreach(println(_))

    val listOfPredicates = ontologyTriples.map(x => x.getPredicate.getLocalName).distinct(2)
    println("List of predicates in the ontology: ")
    listOfPredicates.foreach(println(_))
  }
  def GetNumberOfClasses(ontologyTriples: RDD[graph.Triple]): Double={
    val numOfClasses = ontologyTriples.filter(q => q.getSubject.isURI && q.getObject.isURI && q.getObject.getLocalName == "Class").distinct(2).count()
    numOfClasses
  }
  def GetNumberOfRelations(ontologyTriples: RDD[graph.Triple]): Double={
    val numOfRelations = ontologyTriples.filter(q => q.getObject.isURI && q.getObject.getLocalName == "ObjectProperty").distinct(2).count()

    numOfRelations
  }
  def GetNumberOfAttributes(ontologyTriples: RDD[graph.Triple]): Double={
    val numOfAttributes = ontologyTriples.filter(q => q.getObject.isURI && q.getObject.getLocalName == "DatatypeProperty").distinct(2).count()

    numOfAttributes
  }
  def GetNumberOfSubClasses(ontologyTriples: RDD[graph.Triple]): Double={
    val numOfSubClasses = ontologyTriples.filter(q => q.getSubject.isURI && q.getObject.isURI && q.getPredicate.getLocalName == "subClassOf").distinct(2).count()
//    println("Number of SubClasses "+numOfSubClasses)
    numOfSubClasses

  }
//  def GetNumberOfRelations(triples: RDD[graph.Triple]): Double={
//    val numOfObjectProperty = triples.filter(q => q.getObject.isURI && q.getObject.getLocalName == "ObjectProperty").distinct(2).count()
//
//    val numOfAnnotationProperty = triples.filter(q => q.getObject.isURI && q.getObject.getLocalName == "AnnotationProperty").distinct(2).count()
//    //    sAnnotationProperty.foreach(println(_))
//
//    val numOfDatatypeProperty = triples.filter(q => q.getObject.isURI && q.getObject.getLocalName == "DatatypeProperty").distinct(2).count()
//
//    val numOfRelations = numOfObjectProperty + numOfAnnotationProperty + numOfDatatypeProperty
//
//    numOfRelations
//  }
  def OntologyWithCodeOrText (ontologyTriples: RDD[graph.Triple]): Boolean={
    val classes = ontologyTriples.filter(q => q.getSubject.isURI && q.getObject.isURI && q.getObject.getLocalName == "Class").distinct(2)
    var hasCode = false
    if (classes.first().getSubject.getLocalName.exists(_.isDigit))
      hasCode = true
    else hasCode = false
  hasCode
}

  def RetrieveClassesWithLabels (ontologyTriples: RDD[graph.Triple]): RDD[String]={ //will be applied for ontologies without codes like SEO
    val classesWithoutURIs: RDD[String] = ontologyTriples.find(None, None, Some(NodeFactory.createURI("http://www.w3.org/2002/07/owl#Class")))
      .map(x => x.getSubject.getLocalName)
    classesWithoutURIs
  }
  def RetrieveClassesWithCodesAndLabels(ontologyTriples: RDD[graph.Triple]): RDD[(String, String)]={ //will be applied for ontologies with codes like Multifarm ontologies
    var classes = ontologyTriples.find(None, None, Some(NodeFactory.createURI("http://www.w3.org/2002/07/owl#Class"))).keyBy(_.getSubject.getLocalName)
      .join(ontologyTriples.keyBy(_.getSubject.getLocalName))
      .filter(x=> x._2._2.getPredicate.getLocalName == "label")
      .map(y=> (y._1, y._2._2.getObject.getLiteral.getLexicalForm.split("@").head)).distinct(2)
    classes
  }
  def RetrieveClassesWithoutLabels (o: RDD[graph.Triple]): RDD[String]={ //for classes with local names ex:ekaw-en, edas and SEO ontologies
    val p = new PreProcessing()
    val classesWithoutURIs: RDD[String] = o.map(y=>p.stringPreProcessing(y.getSubject.getLocalName)).distinct().union(o.map{case(x)=> if(x.getObject.isURI)(p.stringPreProcessing(x.getObject.getLocalName))else null}.filter(y => y != null && y != "class")).distinct()
    classesWithoutURIs
  }
  def RetrieveRelationsWithoutCodes(ontologyTriples: RDD[graph.Triple]): RDD[(String, String)]={
    val prop: RDD[(String, String)] = ontologyTriples.filter(q => (q.getObject.isURI && q.getObject.getLocalName == "ObjectProperty") || (q.getObject.isURI && q.getObject.getLocalName == "AnnotationProperty") || (q.getObject.isURI && q.getObject.getLocalName == "DatatypeProperty")|| (q.getObject.isURI && q.getObject.getLocalName == "FunctionalProperty")|| (q.getObject.isURI && q.getObject.getLocalName == "InverseFunctionalProperty")).distinct(2).map(x => (x.getSubject.getLocalName,x.getObject.getLocalName)).distinct(2)
    prop
  }
  def RetrieveRelationsWithCodes(sourceLabelBroadcasting: Broadcast[Map[Node, graph.Triple]], ontologyTriples: RDD[graph.Triple]): RDD[(String, String)]={
    val prop: RDD[graph.Triple] = ontologyTriples.filter(q => (q.getObject.isURI && q.getObject.getLocalName == "ObjectProperty") || (q.getObject.isURI && q.getObject.getLocalName == "AnnotationProperty") || (q.getObject.isURI && q.getObject.getLocalName == "DatatypeProperty")|| (q.getObject.isURI && q.getObject.getLocalName == "FunctionalProperty")|| (q.getObject.isURI && q.getObject.getLocalName == "InverseFunctionalProperty")).distinct(2)
//    println("prop =============>")
//    prop.foreach(println(_))
    val relations: RDD[(String, String)] = prop.map(x => if (sourceLabelBroadcasting.value.contains(x.getSubject))(x.getSubject.getLocalName, sourceLabelBroadcasting.value(x.getSubject).getObject.getLiteral.toString().split("@").head) else (x.getSubject.getLocalName, x.getObject.getLocalName)).distinct(2)
    relations
  }
  def Round(num: Double):Double={
    (num * 100).round / 100.toDouble
  }
}
