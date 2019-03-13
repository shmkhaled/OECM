
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

class Translator (targetClasses: Broadcast[Map[String, Long]]) extends Serializable {
//  def Translate (translatedSourceOntology: RDD[(String, String, String)], translations: RDD[graph.Triple]):RDD[(String, String, String)]={
def Translate (preprocessedSourceClasses: RDD[String],relevantTranslations: RDD[(String, List[String])]): RDD[(String, List[String])]={
  var sourceClassesWithTranslations: RDD[(String, List[String])] = relevantTranslations.keyBy(_._1).leftOuterJoin(preprocessedSourceClasses.zipWithIndex().keyBy((_._1))).map(x=>(x._1,x._2._1._2))
//  sourceClassesWithTranslations.foreach(println(_))
  sourceClassesWithTranslations
  }
  def GetBestTranslation(listOftranslations: List[String]): List[String]={
    val sp = SparkSession.builder.master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    var bestTranslation = List(" ")
    val gS = new GetSimilarity()
    var t: RDD[String] = sp.sparkContext.parallelize(targetClasses.value.map(_._1).toList).cache()
    var translations = sp.sparkContext.parallelize(listOftranslations)
    var crossRDD: RDD[(String, String)] = translations.cartesian(t)
    var sim: RDD[(String, String, Double)] = crossRDD.map(x=>(x._1,x._2,gS.getJaccardStringSimilarity(x._1,x._2))).filter(y=>y._3>=0.3)
//    sim.foreach(println(_))
    var matchedTerms: RDD[String] = sim.map(x=>x._2)
    if (!matchedTerms.isEmpty()){
      bestTranslation = matchedTerms.collect().toList
    }
    else bestTranslation = listOftranslations

    bestTranslation
  }
}
