
import net.sansa_stack.rdf.spark.io._
import org.apache.jena.graph
import org.apache.jena.graph.Node
import org.apache.jena.riot.Lang
import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

//import net.sansa_stack.ml.common.nlp.wordnet

/*
* Created by Shimaa 15.oct.2018
* */

object OntologyEnrichment {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val sparkSession1 = SparkSession.builder
//      .master("spark://172.18.160.16:3090")
                .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    val startTimeMillis = System.currentTimeMillis()
//    println("Start time in millis "+startTimeMillis)
    val inputTarget = "src/main/resources/SEO.nt"
    val inputSource = "src/main/resources/conference-de.nt"
    val lang1: Lang = Lang.NTRIPLES
    val sourceOntology: RDD[graph.Triple] = sparkSession1.rdf(lang1)(inputSource).distinct(2)
    val targetOntology: RDD[graph.Triple] = sparkSession1.rdf(lang1)(inputTarget).distinct(2)
    val runTime = Runtime.getRuntime

    val ontStat = new OntologyStatistics(sparkSession1)
    println("op-0498445-3013448"+"op-0498445-3013448 ".exists(_.isDigit))
    println("################### Statistics of the Source Ontology ###########################")
    ontStat.GetStatistics(sourceOntology)
    println("################### Statistics of the Target Ontology ###########################")
    ontStat.GetStatistics(targetOntology)

    //################# Check if the ontology concepts has code or not ############
    println("################# Check if the ontology concepts has code or not ############")
    var sourceOntologyHasCode = ontStat.OntologyWithCodeOrText(sourceOntology)
    println("Source ontology has code? "+sourceOntologyHasCode)
    var targetOntologyHasCode = ontStat.OntologyWithCodeOrText(targetOntology)
    println("Target ontology has code? "+targetOntologyHasCode)
    println("########################## Replacing classes and properties with their labels ##############################")
//    val sourceOntologyLabels: Map[Node, graph.Triple] = sourceOntology.filter(x=>x.getPredicate.getLocalName == "label").keyBy(_.getSubject).collect().toMap
//    println("All labels ")
//    sourceOntologyLabels.foreach(println(_))
//    val labelBroadcasting: Broadcast[Map[Node, graph.Triple]] = sparkSession1.sparkContext.broadcast(sourceOntologyLabels)
//    val ontoWithLabels = new OntologyWithLabels(labelBroadcasting)

    val ontoRebuild = new OntologyRebuilding(sparkSession1)
    val p = new PreProcessing()
    val sOntology: RDD[(String, String, String)] = ontoRebuild.RebuildOntologyWithLabels(sourceOntology)
    println("############################## Mapped Source Ontology ##############################"+ sOntology.count())
    sOntology.foreach(println(_))
    val tOntology: RDD[(String, String, String)] = ontoRebuild.RebuildOntologyWithLabels(targetOntology)
    println("############################## Mapped Target Ontology ##############################" + tOntology.count())
    tOntology.foreach(println(_))


         //####################### Translation #####################################
      //      var targetClassesWithoutURIs: RDD[String] = targetOntology.map(y=>p.stringPreProcessing(y.getSubject.getLocalName)).distinct().union(targetOntology.map{case(x)=> if(x.getObject.isURI)(p.stringPreProcessing(x.getObject.getLocalName))else null}.filter(y => y != null && y != "class")).distinct()//for classes with local names ex:ekaw-en, edas and SEO ontologies
      //    var tt: RDD[String] = targetOntology.map{case(x)=> if(!x.getObject.isLiteral)(p.stringPreProcessing(x.getObject.getLocalName))else null}.filter(y => y != null && y != "class").distinct()
      //    println("########## new ##############"+ tt.count())
      //    tt.foreach(println(_))
      //    println("########################")

 // Retrieve class name for the source and target ontology (for classes with labels ex:cmt-en, confOf-de and sigkdd-de ontologies)
    val targetClassesWithoutURIs: RDD[String] = ontStat.RetrieveClassesWithLabels(targetOntology).persist(StorageLevel.MEMORY_AND_DISK) //For SEO

//    println("All classes in the target ontology Triples:" + targetClassesWithoutURIs.count())
//    targetClassesWithoutURIs.foreach(println(_))
    var sourceClassesAndRelationsWithURIs = ontStat.RetrieveClassesWithURIsAndLabels(sourceOntology) //applied for ontologies with codes like Multifarm ontologies
//    println("All classes with URIs in the source ontology Triples:" + sourceClassesAndRelationsWithURIs.count())
//    sourceClassesAndRelationsWithURIs.foreach(println(_))
    val targetRelations = ontStat.RetrieveRelationsWithoutURIs(targetOntology)
    println("All relations in the target ontology are: "+targetRelations.count())
    targetRelations.foreach(println(_))
    val sourceRelationsURIs: RDD[(String, String)] = ontStat.RetrieveRelationsWithoutURIs(sourceOntology)
    println("All relations in the source ontology are: "+sourceRelationsURIs.count())
    sourceRelationsURIs.foreach(println(_))
    val sourceOntologyLabels: Map[Node, graph.Triple] = sourceOntology.filter(x=>x.getPredicate.getLocalName == "label").keyBy(_.getSubject).collect().toMap
//    println("All labels ")
//    sourceOntologyLabels.foreach(println(_))
    val sourceLabelBroadcasting: Broadcast[Map[Node, graph.Triple]] = sparkSession1.sparkContext.broadcast(sourceOntologyLabels)
    ontStat.RetrieveRelationsWithURIs(sourceLabelBroadcasting,sourceOntology)


    //    //######################## Second Enrichment from Arabic Ontology ########################
    //      val e = new SecondEnrichment(sparkSession1)
    //      var targetClassesWithoutURIs: RDD[String] = e.GetTargetClassesAfterEnrichment(targetOntology) //for SEO after enrichment
    //######################################################################################################
    println("#################################### Translation step ####################################")
    val t = targetClassesWithoutURIs.zipWithIndex().collect().toMap
    val targetClassesBroadcasting: Broadcast[Map[String, Long]] = sparkSession1.sparkContext.broadcast(t)
    val translate = new Translation(sparkSession1)
    val availableTranslations: RDD[(String, List[String])] = translate.GettingAllAvailableTranslations(args(2))
//    println("All available translations:")
//    availableTranslations.foreach(println(_))
    val sourceClassesWithListOfBestTranslations = availableTranslations.map(x => (x._1,x._2,translate.GetBestTranslation(x._2,targetClassesBroadcasting)))//.cache()
//    println("All sources with list of best translations ")
//    sourceClassesWithListOfBestTranslations.foreach(println(_))
    //sourceClassesWithListOfBestTranslations.coalesce(1).saveAsTextFile("src/main/resources/EvaluationDataset/German/translation")
    //println("Translations should be validated by experts")

    val listOfMatchedTerms: RDD[List[String]] = sourceClassesWithListOfBestTranslations.map(x => x._3.toString().toLowerCase.split(",").toList).filter(y => y.last.exists(_.isDigit)).persist(StorageLevel.MEMORY_AND_DISK)
//    println("List of matched terms ")
//    listOfMatchedTerms.foreach(println(_))

    val sourceClassesWithBestTranslation: RDD[(String, String)] = sourceClassesWithListOfBestTranslations.map(x=>(x._1.toLowerCase,p.stringPreProcessing(x._3.head.toString.toLowerCase.split(",").head))).keyBy(_._1)
      .join(sourceClassesAndRelationsWithURIs)
      .map({case (u, ((uu, tt), s)) => (s,tt.trim.replaceAll(" +", " "))})//.cache()//.filter(!_.isDigit)
//    println("Translated source classes W.R.T the target ontology: ")
//    sourceClassesWithBestTranslation.foreach(println(_))

    println("####################### Recreating the source ontology with using valid translations #####################################")
    val sor = new SourceOntologyReconstruction()
    val translatedSourceOntology = sor.ReconstructOntology(sOntology,sourceClassesWithBestTranslation).persist(StorageLevel.MEMORY_AND_DISK)//.filter(x=>x._2 != "disjointWith").cache()
//
//    println("Source Ontology after translating subject and object classes "+ translatedSourceOntology.count())
//    translatedSourceOntology.distinct().foreach(println(_))

    println("####################### Enrichment Phase #######################")
    val m = new MatchingTwoOntologies(sparkSession1)
    var monolingualTriplesForEnrichment: RDD[(String, String, String)] = m.GetTriplesToBeEnriched(translatedSourceOntology, targetClassesWithoutURIs,listOfMatchedTerms)//.cache()
    println("####################### monolingual source triples needed for enrichment ####################### " +     monolingualTriplesForEnrichment.count())
    monolingualTriplesForEnrichment.foreach(println(_))
//    translatedSourceOntology.coalesce(1).saveAsTextFile("Output/triplesForEnrichment(SEO-Conference)")
    println("####################### Generate multilingual Triples #######################")
    val multilingual = new RetriveMultilingualInfo()
    val foreignLanguageTriples = multilingual.getForeignLangTriples(monolingualTriplesForEnrichment, sourceClassesWithBestTranslation)
    val triplesForEnrichment = monolingualTriplesForEnrichment.union(foreignLanguageTriples)
    println("Number of triples = "+triplesForEnrichment.count())
    triplesForEnrichment.foreach(println(_))

    val endTimeMillis = System.currentTimeMillis()
    val durationMinutes = (endTimeMillis - startTimeMillis) / (1000*60)
    println("runtime = "+durationMinutes+ " minutes")

    sparkSession1.stop
  }
}
