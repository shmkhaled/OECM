import org.apache.spark.rdd.RDD

class SourceOntologyReconstruction {
  def ReconstructOntology (preProcessedSourceOntology: RDD[(String, String, String)], sourceClassesWithTranslation: RDD[(String, String)]):RDD[(String, String, String)]={
//      println("The length of the source Ontology before translation is: "+preProcessedSourceOntology.count())
    var subjectTranslation: RDD[(String, String, String)] = preProcessedSourceOntology.keyBy(_._1).join(sourceClassesWithTranslation)
        .map({
          case (g, ((s, pre, o), e)) => (e, pre, o)
        }).cache()
//          println("Source Ontology after translating the subject class "+ subjectTranslation.count())
//          subjectTranslation.foreach(println(_))
      var triplesWithTypeClass = subjectTranslation.filter(x=>x._2 == "type")


      var translatedSourceOntology = subjectTranslation.keyBy(_._3.toString).join(sourceClassesWithTranslation)
        .map({
          case (g, ((s, pre, o), e)) => (s, pre, e)
        }).union(triplesWithTypeClass).cache()

        translatedSourceOntology
  }

}
