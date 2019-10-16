
import net.sansa_stack.rdf.spark.io._
import org.apache.jena.graph
import org.apache.jena.riot.Lang
import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

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
    val inputTarget = "src/main/resources/SEO.nt"
    val inputSource = "src/main/resources/conference-de.nt"
    val lang1: Lang = Lang.NTRIPLES
    val sourceOntology: RDD[graph.Triple] = sparkSession1.rdf(lang1)(inputSource).distinct()
    val targetOntology: RDD[graph.Triple] = sparkSession1.rdf(lang1)(inputTarget).distinct()
    val runTime = Runtime.getRuntime

    val ontStat = new OntologyStatistics()
    println("################### Statistics of the Source Ontology ###########################")
    ontStat.GetStatistics(sourceOntology)
    println("################### Statistics of the Target Ontology ###########################")
    ontStat.GetStatistics(targetOntology)

    println("########################## Replacing classes and properties with their labels ##############################")
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
          val OC = new OntologyClasses()
//          var targetClassesWithoutURIs: RDD[String] = OC.RetrieveClassesWithLabels(targetOntology) //For SEO2
      val targetClassesWithoutURIs: RDD[String] = ontStat.RetrieveClassesWithLabels(targetOntology) //For SEO
      //    var targetClassesWithoutURIs: RDD[String] = OC.RetrieveClassesWithoutLabels(targetOntology) //for SEO before enrichment

      //    //######################## Second Enrichment from Arabic Ontology ########################
      //      val e = new SecondEnrichment(sparkSession1)
      //      var targetClassesWithoutURIs: RDD[String] = e.GetTargetClassesAfterEnrichment(targetOntology) //for SEO after enrichment



          //          targetOntology.filter(x=>x.getPredicate.getLocalName == "label").map(y=>y.getObject.getLiteral.getLexicalForm.split("@").head)//for classes with labels ex:cmt-en, confOf-de and sigkdd-de ontologies

          println("All classes in the target ontology Triples:" + targetClassesWithoutURIs.count())
          targetClassesWithoutURIs.foreach(println(_))

            //sourceOntology.filter(x=>x.getPredicate.getLocalName == "label").map(y=>y.getObject.getLiteral.getLexicalForm.split("@").head).distinct().collect()

      //    var sourceClassesWithoutURIs: RDD[String] = OC.RetrieveClassesWithLabels(sourceOntology)
      //      println("All classes in the source ontology Triples:" + sourceClassesWithoutURIs.count())
      //      sourceClassesWithoutURIs.foreach(println(_))
      //    if (args(0).contains("-de-")){
      //      sourceClassesWithoutURIs = OC.RetrieveClassesWithLabelsForGerman(sourceOntology, sparkSession1)
      //    }
            var sourceClassesWithURIs = OC.RetrieveClassesWithURIsAndLabels(sourceOntology) //applied for german and arabic ontologies
            println("All classes with URIs in the source ontology Triples:" + sourceClassesWithURIs.count())
            sourceClassesWithURIs.foreach(println(_))

            //    var germanTagger: MaxentTagger = new MaxentTagger("edu/stanford/nlp/models/pos-tagger/german/german-fast.tagger")
      //    var preprocessedSourceClasses: RDD[String] = sparkSession1.sparkContext.parallelize(p.germanPosTag(sourceClassesWithoutURIs,germanTagger)).filter(x=>x.isEmpty == false).cache()
      //  println("All source classes after preprocessing "+preprocessedSourceClasses.count())
      //  preprocessedSourceClasses.foreach(println(_))

            //Read one-to-many translations from csv file
      //    val src = Source.fromFile("src/main/resources/EvaluationDataset/Translations/Translations-conference-de_new.csv")
          //  val src = Source.fromFile("src/main/resources/EvaluationDataset/Translations/Translations-confOf-de.csv")
          //  val src = Source.fromFile("src/main/resources/EvaluationDataset/Translations/Translations-sigkdd-de.csv")
      //    val availableTranslations: RDD[(String, List[String])] = sparkSession1.sparkContext.textFile(args(2)).map(_.split(",").toList).map(x=>(x.head, x.tail))
      //val availableTranslations: RDD[(String, List[String])] = sparkSession1.sparkContext.textFile(args(2)).map(_.split(",").toList).map(x=>(x.head, x.tail.map(y=>p.englishPosTagForString(y))))
          import edu.stanford.nlp.util.logging.RedwoodConfiguration
          RedwoodConfiguration.current.clear.apply()
      val availableTranslations: RDD[(String, List[String])] = sparkSession1.sparkContext.textFile(args(2)).map(_.split(",").toList).map(x=>(x.head, x.tail.drop(1).map(y=>p.englishPosTagForString(y))))
            println("Translations")
            availableTranslations.foreach(println(_))


          val t = targetClassesWithoutURIs.zipWithIndex().collect().toMap
          val dc: Broadcast[Map[String, Long]] = sparkSession1.sparkContext.broadcast(t)
          val trans = new Translator(dc)
      //    val sourceClassesWithAllAvailableTranslations: RDD[(String, List[String])] = trans.Translate(preprocessedSourceClasses, availableTranslations).distinct()
      //    val sourceClassesWithAllAvailableTranslations: RDD[(String, List[String])] = trans.Translate(sourceClassesWithoutURIs, availableTranslations).distinct()
      //
      //    println("All source with all translations "+sourceClassesWithAllAvailableTranslations.count())
      //    sourceClassesWithAllAvailableTranslations.foreach(println(_))

      //    val sourceClassesWithListOfBestTranslations = sourceClassesWithAllAvailableTranslations.map(x => (x._1,x._2,trans.GetBestTranslation(x._2))).cache()
          val sourceClassesWithListOfBestTranslations = availableTranslations.map(x => (x._1,x._2,trans.GetBestTranslation(x._2))).cache()

            println("All sources with list of best translations ")
          sourceClassesWithListOfBestTranslations.take(70).foreach(println(_))
      //    sourceClassesWithListOfBestTranslations.coalesce(1).saveAsTextFile("src/main/resources/EvaluationDataset/German/translation")
      //    println("Translations should be validated by experts")

          var listOfMatchedTerms: RDD[List[String]] = sourceClassesWithListOfBestTranslations.map(x => x._3.toString().toLowerCase.split(",").toList).filter(y => y.last.exists(_.isDigit))
          println("List of matched terms ")
          listOfMatchedTerms.foreach(println(_))

          val validSourceTranslations: RDD[(String, String)] = sourceClassesWithListOfBestTranslations.map(x=>(x._1.toLowerCase,p.stringPreProcessing(x._3.head.toString.toLowerCase.split(",").head))).keyBy(_._1).join(sourceClassesWithURIs).map({case (u, ((uu, tt), s)) => (s,tt.trim.replaceAll(" +", " "))}).cache()//.filter(!_.isDigit)

            /*Experts should validate the translations*/
        //arg(3) = src/main/resources/EvaluationDataset/Translations/ConferenceTranslations_W_R_T_SEO
      //    val validSourceTranslations: RDD[(String, String)] = sparkSession1.sparkContext.textFile(args(3)).map(x=>x.split(",")).map(y=>(y.head.toLowerCase,y.last.toLowerCase))
          println("Validated translated source classes W.R.T SEO: ")
          validSourceTranslations.take(70).foreach(println(_))

          println("####################### Recreating the source ontology with using valid translations #####################################")
          val sor = new SourceOntologyReconstruction()
          var translatedSourceOntology = sor.ReconstructOntology(sOntology,validSourceTranslations)//.filter(x=>x._2 != "disjointWith").cache()
      //
          println("Source Ontology after translating subject and object classes "+ translatedSourceOntology.count())
          translatedSourceOntology.distinct().foreach(println(_))

          println("####################### Matching Two Ontologies #######################")
      //    var targetOntologyWithoutURI: RDD[(String, String, String)] = targetOntology.map{case(x)=> if (x.getObject.isURI)(p.stringPreProcessing(x.getSubject.getLocalName),x.getPredicate.getLocalName,p.stringPreProcessing(x.getObject.getLiteral.toString))else (p.stringPreProcessing(x.getSubject.getLocalName),x.getPredicate.getLocalName,p.stringPreProcessing(x.getObject.getLocalName))}//.filter(x=>x._2 != "type" || x._2 != "comment") //for SEO.nt first version

      //var targetOntologyWithoutURI: RDD[(String, String, String)] = targetOntology.map{case(x)=> if (!x.getObject.isURI)(p.stringPreProcessing(x.getSubject.getLocalName),x.getPredicate.getLocalName,p.stringPreProcessing(x.getObject.getLiteral.toString))else (p.stringPreProcessing(x.getSubject.getLocalName),x.getPredicate.getLocalName,p.stringPreProcessing(x.getObject.getLocalName))}

      //    println("Target ontology without URIs")
      //    targetOntologyWithoutURI.foreach(println(_))
          val m = new MatchingTwoOntologies(sparkSession1)
      //    var triplesForEnrichment: RDD[(String, String, String, Char)] = m.Match(translatedSourceOntology,targetOntologyWithoutURI, targetClassesWithoutURIs).distinct().cache()
      //var triplesForEnrichment= m.GetTriplesToBeEnriched(translatedSourceOntology,targetOntologyWithoutURI, targetClassesWithoutURIs,listOfMatchedTerms).cache()
          var monolingualTriplesForEnrichment: RDD[(String, String, String)] = m.GetTriplesToBeEnriched(translatedSourceOntology, targetClassesWithoutURIs,listOfMatchedTerms).cache()
          println("####################### monolingual source triples needed for enrichment ####################### " +     monolingualTriplesForEnrichment.count())
          monolingualTriplesForEnrichment.foreach(println(_))
      //  translatedSourceOntology.coalesce(1).saveAsTextFile("Output/triplesForEnrichment(SEO-Conference)")
          println("####################### Generate multilingual Triples #######################")
          val multilingual = new RetriveMultilingualInfo()
          val foreignLanguageTriples = multilingual.getForeignLangTriples(monolingualTriplesForEnrichment, validSourceTranslations)
          val triplesForEnrichment = monolingualTriplesForEnrichment.union(foreignLanguageTriples)
          println("Number of triples = "+triplesForEnrichment.count())
          triplesForEnrichment.foreach(println(_))

          val endTimeMillis = System.currentTimeMillis()
          val durationMinutes = (endTimeMillis - startTimeMillis) / (1000*60)
          println("runtime = "+durationMinutes+ " minutes")

    sparkSession1.stop
  }
}
