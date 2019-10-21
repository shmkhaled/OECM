import org.apache.spark.rdd.RDD

class SourceOntologyReconstruction {
  def ReconstructOntology (sOntology: RDD[(String, String, String)], sourceClassesWithBestTranslation: RDD[(String, String)]):RDD[(String, String, String)]={
    //println("The length of the source Ontology before translation is: "+sOntology.count())
//    var xxx = sOntology.keyBy(_._1).join(sourceClassesWithBestTranslation)
//    xxx.foreach(println(_))
    val subjectTranslation: RDD[(String, String, String)] = sOntology.keyBy(_._1).join(sourceClassesWithBestTranslation)
        .map({
          case (g, ((s, pre, o), e)) => (e, pre, o)
        }).cache()
//    println("Source Ontology after translating the subject class "+ subjectTranslation.count())
//    subjectTranslation.foreach(println(_))
    var triplesWithTypeClass = subjectTranslation.filter(x=>x._2 == "type")
    var translatedSourceOntology = subjectTranslation.keyBy(_._3.toString).join(sourceClassesWithBestTranslation)
      .map({
        case (g, ((s, pre, o), e)) => (s, pre, e)
      }).union(triplesWithTypeClass)//.cache()

      translatedSourceOntology
  }

}
