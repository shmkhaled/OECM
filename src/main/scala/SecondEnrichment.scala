import org.apache.jena.graph
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

class SecondEnrichment(sparkSession: SparkSession) {
  def GetTargetClassesAfterEnrichment(targetOntology: RDD[graph.Triple]):RDD[String]={
    val OC = new OntologyClasses()
    var Ct: RDD[String] = OC.RetrieveClassesWithoutLabels(targetOntology) //for SEO before enrichment
    val Te: RDD[(String,String)] = sparkSession.sparkContext.textFile("Output Results/Te(SEO_enXConference_de)Iterative.txt").map(_.split(",")).map(x=>(x.head,x.last))
    var classesInTe = Te.map(x=>x._1).union(Te.map(y=>y._2)).filter(x => x != "Class").distinct()
    var targetClassesWithoutURIs = Ct.union(classesInTe)
    targetClassesWithoutURIs
  }

}
