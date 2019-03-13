import org.apache.jena.graph
import org.apache.spark.rdd.RDD

class Enriching {
  def enrichTargetOntology(triplesToBeAdded: RDD[(String, String, String)], targetOntology: RDD[graph.Triple]): RDD[(String, String, String)] ={
//    var sourceOntologyWithoutURI: RDD[(String, String, String)] = sourceOntology.map{case(x)=> if (x.getObject.isLiteral)(x.getSubject.getLocalName,x.getPredicate.getLocalName,x.getObject.getLiteral.toString)else (x.getSubject.getLocalName,x.getPredicate.getLocalName,x.getObject.getLocalName)}
    var enrichedTargetOntology = targetOntology.map(x=>(x.getSubject.toString,x.getPredicate.toString,x.getObject.toString)).union(triplesToBeAdded)
    enrichedTargetOntology

  }
}
