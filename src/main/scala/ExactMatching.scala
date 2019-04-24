import org.apache.jena.graph
import org.apache.jena.graph.Node
import org.apache.spark.rdd.RDD

/*
* Created by Shimaa 16.10.2018
* */
class ExactMatching {
  def matching(sourceSubOntology: RDD[graph.Triple], targetOntology: RDD[graph.Triple]): Unit ={
    val sourceSize :Int = sourceSubOntology.count().toInt
    val targetSize :Int = targetOntology.count().toInt
    println("======================================")
    println("|        Source Ontology       |")
    println("======================================")
    sourceSubOntology.take(sourceSize).foreach(println(_))
    println("======================================")
    println("|        Target Ontology       |")
    println("======================================")
    targetOntology.take(targetSize).foreach(println(_))
    println("|        mapped source Ontology       |")
    var sourceOntology: RDD[(Node, (Node, Node))] = sourceSubOntology.map(t=> (t.getSubject, (t.getPredicate,t.getObject)))
    sourceOntology.foreach(println(_))

    println("|        mapped target Ontology       |")
    var tOntology: RDD[(Node, (Node, Node))] = targetOntology.map(s=> (s.getSubject, (s.getPredicate,s.getObject)))
    tOntology.foreach(println(_))

    println("|        The intersection between the two ontologies       |")
    var matchOntology: RDD[(Node, (Node, Node))] = sourceOntology.intersection(tOntology)//get full matched triples
    matchOntology.foreach(println(_))
    println("|        The source ontologyTriples after removing the common triples       |")
    sourceOntology = sourceOntology.subtract(matchOntology)//remove full matched triples from the source ontologyTriples
    sourceOntology.foreach(println(_))
//    var source :RDD[graph.Triple] = sourceOntology.map(t=>t._1,t.)

    var matchScore = 0.0
//   var s = sOntology.take(sOntology.count().toInt).apply(1)
//    println("s = "+ s)
//    var t = targetOntology.take(targetOntology.count().toInt).apply(0)
//    println("t = "+ t)
    var i: Int = 0
    var j: Int = 0
    var s: graph.Triple = null
    var t: graph.Triple = null
//    for (i <- 0 to sourceSize-1)
//    {
//      s = sourceSubOntology.take(sourceSize).apply(i)
//      for (j <- 0 to targetSize-1)
//      {
//        t = targetOntology.take(targetSize).apply(j)
//        if (s.matches(t)){
//          matchScore = 1
//          println("<"+s.getSubject,s.getPredicate,s.getObject+"> and <"+t.getSubject,t.getPredicate,t.getObject+ "> Exact match")
//
//        }
//        else if (s.subjectMatches(t.getSubject) || s.subjectMatches(t.getObject) || s.objectMatches(t.getSubject) || s.objectMatches(t.getObject))
//        {
//          matchScore = 0.5
//          println("<"+s.getSubject,s.getPredicate,s.getObject+"> and <"+t.getSubject,t.getPredicate,t.getObject+ "> Partial match")
//        }
//        else println("<"+s.getSubject,s.getPredicate,s.getObject+"> and <"+t.getSubject,t.getPredicate,t.getObject+ "> Non match")
//      }
//    }
  }

}
