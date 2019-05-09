import org.apache.spark.rdd.RDD

class RetriveMultilingualInfo {
  def getForeignLangTriples(monolingualTriplesForEnrichment: RDD[(String, String, String)], validSourceTranslations: RDD[(String, String)]): RDD[(String, String, String)]={
    var subjectClass = monolingualTriplesForEnrichment.map(x=>x._1).distinct()
    println("subject")
    subjectClass.foreach(println(_))
    var objectClass = monolingualTriplesForEnrichment.map(x=>x._3).distinct()
    println("object")
    objectClass.foreach(println(_))
    var allClasses = subjectClass.union(objectClass).distinct().cache()
    println("all classes")
    allClasses.foreach(println(_))
    var rdd1: RDD[(String, ((String, Long), (String, String)))] = allClasses.zipWithIndex().keyBy(_._1).join(validSourceTranslations.keyBy(_._1)).cache()
    println("multilingual")
    rdd1.foreach(println(_))
    var multilingualTriples: RDD[(String, String, String)] = rdd1.map(x => (x._1, "label",x._2._2._2))
    multilingualTriples.foreach(println(_))
    multilingualTriples
  }

}
