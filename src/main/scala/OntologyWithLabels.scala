import org.apache.jena.graph
import org.apache.jena.graph.Node
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

class OntologyWithLabels(labelBroadcasting: Broadcast[Map[Node, graph.Triple]])extends Serializable {
  def RecreateOntologyWithLabels(ontologyTriples: RDD[graph.Triple]): RDD[(String, String, String)] = {
    val processing = new PreProcessing()
    val ontologyWithSubjectLabels: RDD[(Node, Node, Node)] = ontologyTriples.filter(x=>x.getPredicate.getLocalName != "label")
      .map(x => if(labelBroadcasting.value.contains(x.getSubject)) (labelBroadcasting.value(x.getSubject).getObject, x.getPredicate, x.getObject) else (x.getSubject, x.getPredicate, x.getObject))
//    println("After broadcasting" + ontologyWithSubjectLabels.count())
//    ontologyWithSubjectLabels.foreach(println(_))

    val ontologyWithSubjectANDObjectLabels: RDD[(Node, Node, Node)] = ontologyWithSubjectLabels.map(x => if (labelBroadcasting.value.contains(x._3)) (x._1, x._2, labelBroadcasting.value(x._3).getObject) else (x._1, x._2, x._3))
//    println("After second broadcasting" + ontologyWithSubjectANDObjectLabels.count())
//    ontologyWithSubjectANDObjectLabels.foreach(println(_))

    val onto: RDD[(String, String, String)] = ontologyWithSubjectANDObjectLabels.map(x => if (x._3.isURI)(x._1, x._2.getLocalName, x._3.getLocalName) else (x._1, x._2.getLocalName, processing.stringPreProcessing2(x._3.toString)))
      .map(y => if (y._1.isURI) (y._1.getLocalName, y._2, y._3) else (processing.stringPreProcessing2(y._1.toString), y._2, y._3))
//    onto.foreach(println(_))

    onto

  }
}
