
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
//    var englishTagger: MaxentTagger = new MaxentTagger("edu/stanford/nlp/models/pos-tagger/english-left3words/english-left3words-distsim.tagger")
//    println(englishTagger.tagString("speaker is invited"))
////    println(englishTagger.tagString("applicant conference who paid early conference fees"))
////    println(englishTagger.tagString("early paid applicant"))
////    println(englishTagger.tagString("the"))
////    println(englishTagger.tagString("for"))
////
//    var pppp = new PreProcessing()
//    println(pppp.englishPosTagForString("speaker is invited"))
//////
//        val gS = new GetSimilarity()
////////
////////    var preee = new PreProcessing()
////////    println(preee.sentenceLemmatization("review preference"))
////////    println(preee.sentenceLemmatization("review question"))
//    var sent1 = "proceeding"
//    var sent2 = "conference proceeding"
////    println(gS.getSimilarity(pppp.stringPreProcessing(sent1),pppp.stringPreProcessing(sent2)))
//      println(gS.getPathSimilarity("proceeding","conference"))
//      println(gS.getPathSimilarity("proceed","conference"))
//
//    println("Path similarity between "+sent1+" and "+sent2+" = "+gS.sentenceSimilarity(sent1,sent2))
//    println("Path similarity between "+sent2+" and "+sent1+" = "+gS.sentenceSimilarity(sent2,sent1))
//    println("Symmetric similarity = "+gS.symmetricSentenceSimilarity(sent1,sent2))
//////
//        println("Path similarity = "+gS.getPathSimilarity("review","question"))
//        println("Path similarity = "+gS.getPathSimilarity("preference","review"))
//        println("Path similarity = "+gS.getPathSimilarity("preference","question"))
//        println("Path similarity = "+gS.getPathSimilarity("candidate","conference"))


    //    println("Path similarity = "+semSim.getPathSimilarity("workshop","chair"))
//    println("Path similarity = "+semSim.getPathSimilarity("chair","proposal"))


    //    println("Path similarity = "+semSim.getPathSimilarity("cat","dog"))
//    println("Path similarity = "+semSim.getPathSimilarity("cat","are"))
//    println("Path similarity = "+semSim.getPathSimilarity("cat","awesome"))
//
//    println("Path similarity = "+semSim.getPathSimilarity("are","dog"))
//    println("Path similarity = "+semSim.getPathSimilarity("are","are"))
//    println("Path similarity = "+semSim.getPathSimilarity("are","awesome"))
//
//    println("Path similarity = "+semSim.getPathSimilarity("beautiful","dog"))
//    println("Path similarity = "+semSim.getPathSimilarity("beautiful","are"))
//    println("Path similarity = "+semSim.getPathSimilarity("beautiful","awesome"))
//
//    println("Path similarity = "+semSim.getPathSimilarity("animal","dog"))
//    println("Path similarity = "+semSim.getPathSimilarity("animal","are"))
//    println("Path similarity = "+semSim.getPathSimilarity("animal","awesome"))

//    println(semSim.symmetricSentenceSimilarity("Cat are beautiful animal", "Dog are awesome"))
//    println(semSim.sentenceSimilarity("workshop chair","workshop proposals"))
//    println(semSim.sentenceSimilarity("workshop proposals","workshop chair"))
//
//    println(semSim.symmetricSentenceSimilarity("workshop chair","workshop proposals"))

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val sparkSession1 = SparkSession.builder
//      .master("spark://172.18.160.16:3090")
                .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    val startTimeMillis = System.currentTimeMillis()
    //############################# Inputs for Evaluation #######################
    //var inputSource = "src/main/resources/EvaluationDataset/German/confOf-de-classes_updated.nt"
    //var inputSource = "src/main/resources/EvaluationDataset/German/sigkdd-de-classes_updated.nt"
    //val inputSource = "src/main/resources/EvaluationDataset/German/conference-de-classes_updated.nt"
    val inputSource = args(0)
    //var inputTarget = "src/main/resources/EvaluationDataset/English/cmt-en-classes_updated.nt"
    //var inputTarget = "src/main/resources/EvaluationDataset/English/ekaw-en-classes_updated.nt"
    //var inputTarget = "src/main/resources/EvaluationDataset/English/edas-en-classes_updated.nt"
    //val inputTarget = "src/main/resources/CaseStudy/SEO_classes.nt"
    val inputTarget = args(1)

    val lang1: Lang = Lang.NTRIPLES
    val sourceOntology: RDD[graph.Triple] = sparkSession1.rdf(lang1)(inputSource).distinct()
    val targetOntology: RDD[graph.Triple] = sparkSession1.rdf(lang1)(inputTarget).distinct()
    val runTime = Runtime.getRuntime

    val ontStat = new OntologyStatistics()
    println("################### Statistics of the Source Ontology ###########################")
    ontStat.GetStatistics(sourceOntology)
    println("################### Statistics of the Target Ontology ###########################")
    ontStat.GetStatistics(targetOntology)

    println("########################## PreProcessing ##############################")
    val p = new PreProcessing()
//    val sOntology: RDD[(String, String, String)] = p.RecreateSourceGermanOntologyWithClassLabels(sourceOntology).cache()
    val sOntology: RDD[(String, String, String)] = p.RecreateOntologyWithClassLabels(sourceOntology).cache()
        println("############################## Mapped Source Ontology ##############################"+ sOntology.count())
    sOntology.foreach(println(_))

    var tOntology: RDD[(String, String, String)] = p.RecreateOntologyWithClassLabels(targetOntology).cache() // should applied if the classes with codes and labels
    println("############################## Mapped Target Ontology ##############################" + tOntology.count())
    tOntology.foreach(println(_))

//    println("======================================")
//    println("|        Predefined Properties       |")
//    println("======================================")
//    println("All available predicates in the source ontology:")
    val sourcePredicatesWithoutURIs = sourceOntology.map(_.getPredicate.getLocalName).distinct()
//    sourcePredicatesWithoutURIs.foreach(println(_))
//    println("Please enter the properties without URIs separated by ',':")
//    val line=scala.io.StdIn.readLine()
//    val predefinedProperty: Array[String] = line.split(',')
    val predefinedProperty: Array[String] = Array("subClassOf", "type")//line.split(',')
//    predefinedProperty.foreach(println(_))
//    println(predefinedProperty.take(predefinedProperty.length).apply(0) +" and " +predefinedProperty.take(predefinedProperty.length).apply(1))
//    val uPredicate1 = predefinedProperty.take(predefinedProperty.length).apply(0)
//    val uPredicate2 = predefinedProperty.take(predefinedProperty.length).apply(1)

    //############# Subgraph Extraction #################
    val subGrapgh = new SubgraphExtraction()
    val subOntology = subGrapgh.extract(sOntology, predefinedProperty)
//    println("############# Sub-source Ontology ################# "+subOntology.count())
//    subOntology.foreach(println(_))
//    var sourceClasses: RDD[Node] = subOntology.getSubjects.union(subOntology.getObjects().filter(x=>x.isURI)).distinct()
//    println("All classes with URIs in the source ontology Triples:")
//    sourceClasses.foreach(println(_))
//    println("All classes in the source ontologyTriples:")
//    var classes: RDD[String] = sourceClasses.map(_.getLocalName).distinct()
//  classes.foreach(println(_))


//    var targetClasses: RDD[String] = targetOntology.getSubjects.map(_.getLocalName).distinct()
//    println("All classes in the target ontologyTriples:")
//    targetClasses.foreach(println(_))

    //####################### Translation #####################################
//      var targetClassesWithoutURIs: RDD[String] = targetOntology.map(y=>p.stringPreProcessing(y.getSubject.getLocalName)).distinct().union(targetOntology.map{case(x)=> if(x.getObject.isURI)(p.stringPreProcessing(x.getObject.getLocalName))else null}.filter(y => y != null && y != "class")).distinct()//for classes with local names ex:ekaw-en, edas and SEO ontologies
//    var tt: RDD[String] = targetOntology.map{case(x)=> if(!x.getObject.isLiteral)(p.stringPreProcessing(x.getObject.getLocalName))else null}.filter(y => y != null && y != "class").distinct()
//    println("########## new ##############"+ tt.count())
//    tt.foreach(println(_))
//    println("########################")

    // Retrieve class name for the source and target ontology (for classes with labels ex:cmt-en, confOf-de and sigkdd-de ontologies)
    val OC = new OntologyClasses()
//    var targetClassesWithoutURIs: RDD[String] = OC.RetrieveClassesWithLabels(targetOntology)
    var targetClassesWithoutURIs: RDD[String] = OC.RetrieveClassesWithoutLabels(targetOntology)

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
    println("###############################")


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

    val validSourceTranslationsByExperts: RDD[(String, String)] = sourceClassesWithListOfBestTranslations.map(x=>(x._1.toLowerCase,p.stringPreProcessing(x._3.head.toString.toLowerCase.split(",").head))).keyBy(_._1).join(sourceClassesWithURIs).map({case (u, ((uu, tt), s)) => (s,tt.trim.replaceAll(" +", " "))})//.filter(!_.isDigit)

      /*Experts should validate the translations*/
  //arg(3) = src/main/resources/EvaluationDataset/Translations/ConferenceTranslations_W_R_T_SEO
//    val validSourceTranslationsByExperts: RDD[(String, String)] = sparkSession1.sparkContext.textFile(args(3)).map(x=>x.split(",")).map(y=>(y.head.toLowerCase,y.last.toLowerCase))
    println("Validated translated source classes W.R.T SEO: ")
    validSourceTranslationsByExperts.take(70).foreach(println(_))
//    println("####################### ")
//    var tttttt: RDD[(String, String)] = sourceClassesWithListOfBestTranslations.map(x=>(x._1.toLowerCase,p.stringPreProcessing(x._3.head.toString.toLowerCase.split(",").head))).keyBy(_._1).join(sourceClassesWithURIs).map({
//        case (u, ((uu, tt), s)) => (s,tt)})
//      tttttt.foreach(println(_))
    println("####################### Recreating the source ontology with using valid translations #####################################")
    val sor = new SourceOntologyReconstruction()
    var translatedSourceOntology = sor.ReconstructOntology(sOntology,validSourceTranslationsByExperts)//.filter(x=>x._2 != "disjointWith").cache()
//
    println("Source Ontology after translating subject and object classes "+ translatedSourceOntology.count())
    translatedSourceOntology.distinct().foreach(println(_))
    //############# ExactMatching #################

    println("####################### Matching Two Ontologies #######################")
//    var targetOntologyWithoutURI: RDD[(String, String, String)] = targetOntology.map{case(x)=> if (x.getObject.isURI)(p.stringPreProcessing(x.getSubject.getLocalName),x.getPredicate.getLocalName,p.stringPreProcessing(x.getObject.getLiteral.toString))else (p.stringPreProcessing(x.getSubject.getLocalName),x.getPredicate.getLocalName,p.stringPreProcessing(x.getObject.getLocalName))}//.filter(x=>x._2 != "type" || x._2 != "comment") //for SEO.nt first version

//var targetOntologyWithoutURI: RDD[(String, String, String)] = targetOntology.map{case(x)=> if (!x.getObject.isURI)(p.stringPreProcessing(x.getSubject.getLocalName),x.getPredicate.getLocalName,p.stringPreProcessing(x.getObject.getLiteral.toString))else (p.stringPreProcessing(x.getSubject.getLocalName),x.getPredicate.getLocalName,p.stringPreProcessing(x.getObject.getLocalName))}

//    println("Target ontology without URIs")
//    targetOntologyWithoutURI.foreach(println(_))
    val m = new MatchingTwoOntologies(sparkSession1)
//    var triplesForEnrichment: RDD[(String, String, String, Char)] = m.Match(translatedSourceOntology,targetOntologyWithoutURI, targetClassesWithoutURIs).distinct().cache()
//var triplesForEnrichment= m.GetTriplesToBeEnriched(translatedSourceOntology,targetOntologyWithoutURI, targetClassesWithoutURIs,listOfMatchedTerms).cache()
    var triplesForEnrichment= m.GetTriplesToBeEnriched(translatedSourceOntology, targetClassesWithoutURIs,listOfMatchedTerms)


    println("####################### source triples needed for enrichment ####################### " + triplesForEnrichment.count())
//    println(triplesForEnrichment.count()+ " triples. Triples with flag 'E' are needed to enrich the target ontology. Triples with flag 'A' are new triples will be added to the target ontology.")
    triplesForEnrichment.foreach(println(_))
//    translatedSourceOntology.coalesce(1).saveAsTextFile("Output/triplesForEnrichment(SEO-Conference)")

    val endTimeMillis = System.currentTimeMillis()
    val durationSeconds = (endTimeMillis - startTimeMillis) / 1000
    println("runtime = "+durationSeconds+ " seconds")

    sparkSession1.stop
  }
}
