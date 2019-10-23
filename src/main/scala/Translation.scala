import edu.stanford.nlp.util.logging.RedwoodConfiguration
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

class Translation (sparkSession: SparkSession) extends Serializable{
  def GettingAllAvailableTranslations(filePathForOfflineTranslation: String): RDD[(String, List[String])]= {
    val p = new PreProcessing()
    RedwoodConfiguration.current.clear.apply()
//    val availableTranslations: RDD[(String, List[String])] = sparkSession.sparkContext.textFile(filePathForOfflineTranslation)
//      .map(_.split(",").toList)
//      .map(x => (x.head, x.tail.drop(1)
//        .map(y => p.englishPosTagForString(y))))
    val availableTranslations: RDD[(String, List[String])] = sparkSession.sparkContext.textFile(filePathForOfflineTranslation)
      .map(_.split(",").toList)
      .map(x => (x.head, x.tail.drop(1)))
    availableTranslations
  }
  def GetBestTranslationForClass(listOfTranslations: List[String], targetClassesBroadcasting: Broadcast[Map[String, Long]]): List[Any]={
    val sp = SparkSession.builder
      //      .master("spark://172.18.160.16:3090")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    var bestTranslation: List[Any] = List(" "," ", 0.0)
    //    var bestTranslation: List[String] = List(" ")

    val gS = new GetSimilarity()
    var t: RDD[String] = sp.sparkContext.parallelize(targetClassesBroadcasting.value.map(_._1).toList)//.cache()
    var translations = sp.sparkContext.parallelize(listOfTranslations)
    var crossRDD: RDD[(String, String)] = translations.cartesian(t)
    //    println("The cross RDD is:")
    //    crossRDD.foreach(println(_))
    //    var sim: RDD[(String, String, Double)] = crossRDD.map(x=>(x._1,x._2,gS.getJaccardStringSimilarity(x._1,x._2))).filter(y=>y._3>=0.3)
    //    var sim: RDD[(String, String, Double)] = crossRDD.map(x=>(x._1,x._2,semSim.getPathSimilarity(x._1.split(" ").last,x._2.split(" ").last))).filter(y=>y._3>=0.6)
    var sim: RDD[(String, String, Double)] = crossRDD.map(x=>(x._1.toLowerCase,x._2.toLowerCase,gS.getSimilarity(x._1,x._2))).filter(y=>y._3>0.9)
//    sim.foreach(println(_))
    var matchedTerms: RDD[(String, String, Double)] = sim//.map(x=>(x._2,x._3))
    if (!matchedTerms.isEmpty()){
      bestTranslation = matchedTerms.collect().toList//.map(x=>(x._1))
    }
    else bestTranslation = listOfTranslations

    bestTranslation
  }
  def GetTranslationForRelation(availableTranslations: RDD[(String, List[String])], sourceRelations: RDD[(String, String)]): RDD[(String, String, String)]={
    val relationsWithTranslation: RDD[(String, String, String)] = sourceRelations.keyBy(_._1).join(availableTranslations.keyBy(_._1)).map(x => (x._1, x._2._1._2, x._2._2._2.head.toLowerCase))
    relationsWithTranslation
  }
}
