import net.sansa_stack.rdf.spark.io._
import org.apache.jena.graph
import org.apache.jena.riot.Lang
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object InstanceEnrichment {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val sp = SparkSession.builder //      .master("spark://172.18.160.16:3090")
      .master("local[*]").config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").getOrCreate()
    val startTimeMillis = System.currentTimeMillis()
        println("Start time in millis "+startTimeMillis)
    val labels = "src/main/resources/DBpedia-3.9/DBpedia-3.9-German/labels_de.nt"
    val lang1: Lang = Lang.NTRIPLES
    val labelGraph: RDD[graph.Triple] = sp.rdf(lang1)(labels).distinct()
    val runTime = Runtime.getRuntime
    val instanceStat = new InstanceStatistics()
    println("################### Statistics of the Target Ontology ###########################")
    instanceStat.GetStatistics(labelGraph)
    val AllLabels: RDD[String] = labelGraph.map(x => x.getObject.getLiteral.toString)
    AllLabels.coalesce(1, shuffle = true).saveAsTextFile("Output/AllLabels")
  }

}
