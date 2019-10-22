import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

class SourceOntologyReconstruction {
  def ReconstructOntology (sOntology: RDD[(String, String, String)], ontologyElements: RDD[(String, String)]):RDD[(String, String, String)]={
//    println("The length of source ontology before translation = "+sOntology.count())
    val subjectTranslation: RDD[(String, String, String)] = sOntology.keyBy(_._1).join(ontologyElements)
        .map({
          case (g, ((s, pre, o), e)) => (e, pre, o)
        }).persist(StorageLevel.MEMORY_AND_DISK)
//    println("Source Ontology after translating the subject "+ subjectTranslation.count())
//    subjectTranslation.foreach(println(_))
    val triplesWithTypeAngRangeProperty = subjectTranslation.filter(x=>x._2 == "type" || x._3 == "string"|| x._3 == "int"|| x._3 == "date"|| x._3 == "Thing")
    val translatedSourceOntology = subjectTranslation.keyBy(_._3.toString).join(ontologyElements)
      .map({
        case (g, ((s, pre, o), e)) => (s, pre, e)
      }).union(triplesWithTypeAngRangeProperty)//.cache()

      translatedSourceOntology
  }

}
