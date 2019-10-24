import org.apache.jena.graph
import org.apache.jena.graph.NodeFactory
import org.apache.spark.rdd.RDD

class GraphCreating {
  def createGraph (triplesForEnrichmentWithURIs: RDD[(String, String, String)]): RDD[graph.Triple]={
    val g: RDD[graph.Triple] = triplesForEnrichmentWithURIs.map{case x => graph.Triple.create(
      NodeFactory.createURI(x._1),
      NodeFactory.createURI(x._2),
      NodeFactory.createURI(x._3)
    )}
    g
  }

}
