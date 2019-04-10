import edu.stanford.nlp.tagger.maxent.MaxentTagger
import org.apache.jena.graph
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

class OntologyClasses {
  def RetrieveClassesWithLabels (o: RDD[graph.Triple]): RDD[String]={
    var classesWithoutURIs = o.filter(x=>x.getPredicate.getLocalName == "label").map(y=>y.getObject.getLiteral.getLexicalForm.split("@").head).distinct()
//    println("All classes in the source ontology Triples:" + sourceClassesWithoutURIs.size)
//    sourceClassesWithoutURIs.foreach(println(_))
    classesWithoutURIs
  }
  def RetrieveClassesWithURIsAndLabels (o: RDD[graph.Triple]): RDD[(String, String)]={
    var classes: RDD[(String, String)] = o.filter(x=>x.getPredicate.getLocalName == "label").map(y=>(y.getSubject.getURI.split("#").last,y.getObject.getLiteral.getLexicalForm.split("@").head.toLowerCase)).distinct()
    //    println("All classes in the source ontology Triples:" + sourceClassesWithoutURIs.size)
    //    sourceClassesWithoutURIs.foreach(println(_))
    classes
  }
  def RetrieveClassesWithoutLabels (o: RDD[graph.Triple]): RDD[String]={
    val p = new PreProcessing()
    var classesWithoutURIs: RDD[String] = o.map(y=>p.stringPreProcessing(y.getSubject.getLocalName)).distinct().union(o.map{case(x)=> if(x.getObject.isURI)(p.stringPreProcessing(x.getObject.getLocalName))else null}.filter(y => y != null && y != "class")).distinct()//for classes with local names ex:ekaw-en, edas and SEO ontologies
    classesWithoutURIs
  }
  def RetrieveClassesWithLabelsForGerman(o: RDD[graph.Triple], sparkSession1: SparkSession): RDD[String]={
    var germanTagger: MaxentTagger = new MaxentTagger("edu/stanford/nlp/models/pos-tagger/german/german-fast.tagger")
    val p = new PreProcessing()

    var classesWithoutURIs = o.filter(x=>x.getPredicate.getLocalName == "label").map(y=>y.getObject.getLiteral.getLexicalForm.split("@").head.toLowerCase).distinct().collect()
    //    println("All classes in the source ontology Triples:" + sourceClassesWithoutURIs.size)
    //    sourceClassesWithoutURIs.foreach(println(_))
    var preprocessedSourceClasses: RDD[String] = sparkSession1.sparkContext.parallelize(p.germanPosTag(classesWithoutURIs,germanTagger)).filter(x=>x.isEmpty == false).cache()
    println("All source classes after preprocessing "+preprocessedSourceClasses.count())
    preprocessedSourceClasses.foreach(println(_))
    preprocessedSourceClasses
  }
}
