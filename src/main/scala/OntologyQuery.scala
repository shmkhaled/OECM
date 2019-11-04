import net.sansa_stack.query.spark.query._
import net.sansa_stack.rdf.spark.io._
import org.apache.jena.graph
import org.apache.jena.riot.Lang
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
/*
* Create by Shimee 4 November 2019
* */
class OntologyQuery (sparkSession: SparkSession) extends Serializable {
  def excuteQuery2(ontologyTriples : RDD[graph.Triple])={
    val sparqlQuery = """SELECT ?s ?p ?o
WHERE {?s ?p ?o }
LIMIT 10"""
    val result: DataFrame = ontologyTriples.sparql(sparqlQuery)
//    z.show(result)
    println("SparqlQuery result:")
    result.show()
  }
  def excuteQuery()={
    val input = "src/main/resources/SEO.nt"
    val lang = Lang.NTRIPLES
    val triples = sparkSession.rdf(lang)(input)
    val sparqlQuery = """SELECT * WHERE {
 |  ?s ?p ?o .
 |}
LIMIT 10"""
    val result = triples.sparql(sparqlQuery)
//    result.show()
  }
}
