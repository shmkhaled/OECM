import org.apache.spark.rdd.RDD

class RetriveMultilingualInfo {
  def getForeignLangTriples(monolingualTriplesForEnrichment: RDD[(String, String, String)], validSourceTranslations: RDD[(String, String)]): RDD[(String, String, String)]={
    var subjectClass = monolingualTriplesForEnrichment.map(x=>x._1)
    var objectClass = monolingualTriplesForEnrichment.map(x=>x._3)
    var allClasses = subjectClass.union(objectClass).distinct()
    var rdd1: RDD[(String, (Long, String))] = allClasses.zipWithIndex().join(validSourceTranslations)
    var multilingualTriples: RDD[(String, String, String)] = rdd1.map(x => (x._1, "label",x._2._2))
    multilingualTriples.foreach(println(_))
    multilingualTriples
  }

}
