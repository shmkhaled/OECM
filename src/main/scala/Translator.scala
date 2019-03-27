
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

class Translator (targetClasses: Broadcast[Map[String, Long]]) extends Serializable {
def Translate (preprocessedSourceClasses: RDD[String], availableTranslations: RDD[(String, List[String])]): RDD[(String, List[String])]={
  var sourceClassesWithTranslations: RDD[(String, List[String])] = availableTranslations.keyBy(_._1).leftOuterJoin(preprocessedSourceClasses.zipWithIndex().keyBy((_._1))).map(x=>(x._1,x._2._1._2))
//  sourceClassesWithTranslations.foreach(println(_))
  sourceClassesWithTranslations
  }

  def GetBestTranslation(listOfTranslations: List[String]): List[Any]={
    val sp = SparkSession.builder.master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    var bestTranslation: List[Any] = List(" ", 0.0)
    val gS = new GetSimilarity()
    val semSim = new SemanticSimilarity()
    var t: RDD[String] = sp.sparkContext.parallelize(targetClasses.value.map(_._1).toList).cache()
    var translations = sp.sparkContext.parallelize(listOfTranslations)
    var crossRDD: RDD[(String, String)] = translations.cartesian(t)
//    var sim: RDD[(String, String, Double)] = crossRDD.map(x=>(x._1,x._2,gS.getJaccardStringSimilarity(x._1,x._2))).filter(y=>y._3>=0.3)
    var sim: RDD[(String, String, Double)] = crossRDD.map(x=>(x._1,x._2,semSim.getPathSimilarity(x._1.split(" ").last,x._2.split(" ").last))).filter(y=>y._3>=0.6)
    sim.foreach(println(_))
    var matchedTerms: RDD[(String, Double)] = sim.map(x=>(x._2,x._3))
    if (!matchedTerms.isEmpty()){
      bestTranslation = matchedTerms.collect().toList
    }
    else bestTranslation = listOfTranslations

    bestTranslation
  }
}
