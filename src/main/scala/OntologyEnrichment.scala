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
    val sparkSession1 = SparkSession.builder //      .master("spark://172.18.160.16:3090")
      .master("local[*]").config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").getOrCreate()
    val startTimeMillis = System.currentTimeMillis()
    //    println("Start time in millis "+startTimeMillis)
    val inputTarget = "src/main/resources/SEO.nt"
//    val inputTarget = "src/main/resources/EvaluationDataset/English/edas-en.nt"
    val inputSource = "src/main/resources/conference-de.nt"
    val lang1: Lang = Lang.NTRIPLES
    val sourceOntology: RDD[graph.Triple] = sparkSession1.rdf(lang1)(inputSource).distinct(2)
    val targetOntology: RDD[graph.Triple] = sparkSession1.rdf(lang1)(inputTarget).distinct(2)
    val runTime = Runtime.getRuntime

    val ontStat = new OntologyStatistics(sparkSession1)
    println("################### Statistics of the Source Ontology ###########################")
    ontStat.GetStatistics(sourceOntology)
    println("################### Statistics of the Target Ontology ###########################")
    ontStat.GetStatistics(targetOntology)

    println("################# Check if the ontology concepts has code or not ############")
    var sourceOntologyHasCode = ontStat.OntologyWithCodeOrText(sourceOntology)
    println("Source ontology has code? " + sourceOntologyHasCode)
    var targetOntologyHasCode = ontStat.OntologyWithCodeOrText(targetOntology)
    println("Target ontology has code? " + targetOntologyHasCode)

    println("########################## Replacing classes and properties with their labels ##############################")
    val ontoRebuild = new OntologyRebuilding(sparkSession1)
    val p = new PreProcessing()

    val sOntology: RDD[(String, String, String)] = ontoRebuild.RebuildOntologyWithLabels(sourceOntology) //.persist(StorageLevel.MEMORY_AND_DISK)
    //    println("############################## Mapped Source Ontology ##############################"+ sOntology.count())
    //    sOntology.foreach(println(_))

    val tOntology: RDD[(String, String, String)] = ontoRebuild.RebuildOntologyWithLabels(targetOntology) //    println("############################## Mapped Target Ontology ##############################" + tOntology.count())
    //    tOntology.foreach(println(_))

    // Retrieve class and relation names for the source and target ontology
    val targetClassesWithoutCodes: RDD[String] = ontStat.RetrieveClassesWithLabels(targetOntology).persist(StorageLevel.MEMORY_AND_DISK) //For SEO
        println("All classes in the target ontology Triples:" + targetClassesWithoutCodes.count())
        targetClassesWithoutCodes.foreach(println(_))
//    targetClassesWithoutCodes.map(x => p.splitCamelCase(x).toLowerCase).coalesce(1, shuffle = true).saveAsTextFile("Output/TargetClasses")

    val targetRelationsWithoutCodes: RDD[(String, String)] = ontStat.RetrieveRelationsWithoutCodes(targetOntology)
    println("All relations in the target ontology are: "+targetRelationsWithoutCodes.count())
    targetRelationsWithoutCodes.foreach(println(_))
//    targetRelationsWithoutCodes.map(x => p.splitCamelCase(x._1).toLowerCase).coalesce(1, shuffle = true).saveAsTextFile("Output/TargetRelations")

    val sourceClassesWithCodes: RDD[(String, String)] = ontStat.RetrieveClassesWithCodesAndLabels(sourceOntology) //applied for ontologies with codes like Multifarm ontologies
    println("All classes with URIs in the source ontology Triples:" + sourceClassesWithCodes.count())
    sourceClassesWithCodes.foreach(println(_))
//    sourceClassesWithCodes.map(x => x._2.toLowerCase).coalesce(1, shuffle = true).saveAsTextFile("Output/SourceClasses")

    val sourceOntologyLabels: Map[Node, graph.Triple] = sourceOntology.filter(x => x.getPredicate.getLocalName == "label").keyBy(_.getSubject).collect().toMap //    println("All labels in the source ontology")
    //    sourceOntologyLabels.foreach(println(_))

    val sourceLabelBroadcasting: Broadcast[Map[Node, graph.Triple]] = sparkSession1.sparkContext.broadcast(sourceOntologyLabels)

    val sourceRelations: RDD[(String, String)] = ontStat.RetrieveRelationsWithCodes(sourceLabelBroadcasting, sourceOntology)
    println("All relations in the source ontology are: " + sourceRelations.count())
    sourceRelations.foreach(println(_))
//    sourceRelations.map(x => x._2.toLowerCase).coalesce(1, shuffle = true).saveAsTextFile("Output/SourceRelations")

    println("======================================")
    println("|       Translation Phase      |")
    println("======================================")
    // ####################### Class Translation #######################
    val t = targetClassesWithoutCodes.zipWithIndex().collect().toMap
    val targetClassesBroadcasting: Broadcast[Map[String, Long]] = sparkSession1.sparkContext.broadcast(t)
    val translate = new ClassSimilarity(sparkSession1)
    val availableTranslations: RDD[(String, List[String])] = translate.GettingAllAvailableTranslations(args(2)) //    println("All available translations:")
    //    availableTranslations.foreach(println(_))
    val sourceClassesWithListOfBestTranslations = availableTranslations.map(x => (x._1, x._2, translate.GetBestTranslationForClass(x._2, targetClassesBroadcasting))).persist(StorageLevel.MEMORY_AND_DISK)
    println("All source classes with list of best translations ")
    sourceClassesWithListOfBestTranslations.foreach(println(_))
    //sourceClassesWithListOfBestTranslations.coalesce(1).saveAsTextFile("src/main/resources/EvaluationDataset/German/translation")
    val listOfMatchedClasses: RDD[List[String]] = sourceClassesWithListOfBestTranslations.map(x => x._3.toString().toLowerCase.split(",").toList).filter(y => y.last.exists(_.isDigit)).persist(StorageLevel.MEMORY_AND_DISK)
    println("List of matched classes between source and target ontologies: "+listOfMatchedClasses.count())
    listOfMatchedClasses.foreach(println(_))
    val sourceClassesWithBestTranslation: RDD[(String, String, String)] = sourceClassesWithListOfBestTranslations.map(x => (x._1.toLowerCase, p.stringPreProcessing(x._3.head.toString.toLowerCase.split(",").head))).keyBy(_._1).join(sourceClassesWithCodes).map({ case (u, ((uu, tt), s)) => (u, s, tt.trim.replaceAll(" +", " ")) }) //.cache()//.filter(!_.isDigit)
    println("Translated source classes W.R.T the target ontology: ")
    sourceClassesWithBestTranslation.foreach(println(_))

    //    ####################### Relation Translation #######################
    val relationsWithTranslation: RDD[(String, String, String)] = translate.GetTranslationForRelation(availableTranslations, sourceRelations)
    println("Relations with translations: ")
    relationsWithTranslation.foreach(println(_))

    println("####################### Recreating the source ontology with using valid translations #####################################")
    val ontologyElementsWithoutCodes: RDD[(String, String)] = sourceClassesWithBestTranslation.map(x => (x._2, x._3)).union(relationsWithTranslation.map(y => (y._2, y._3)))

    val sor = new SourceOntologyReconstruction()
    val translatedSourceOntology = sor.ReconstructOntology(sOntology, ontologyElementsWithoutCodes).persist(StorageLevel.MEMORY_AND_DISK)
    println("Source Ontology after translating classes and relations " + translatedSourceOntology.count())
    translatedSourceOntology.distinct().foreach(println(_))

    val listOfSourcePredicates = translatedSourceOntology.map(x => x._2).distinct(2)
    println("List of predicates in the source ontology: ")
    listOfSourcePredicates.foreach(println(_))

    val listOfSourcePredicatesBroadcasting: Broadcast[Map[String, Long]] = sparkSession1.sparkContext.broadcast(listOfSourcePredicates.zipWithIndex().collect().toMap)
    val translatedSourceOntologyRelationTriples: RDD[(String, String, String)] = translatedSourceOntology.filter(
      x => listOfSourcePredicatesBroadcasting.value.contains(x._2) && x._2 != "subClassOf" && x._2 != "disjointWith" && x._3 != "Class")
    println("Translated relations of the source ontology" + translatedSourceOntologyRelationTriples.count())
    translatedSourceOntologyRelationTriples.foreach(println(_))

    val listOfRelationsInSourceOntology: RDD[String] = translatedSourceOntologyRelationTriples.map(x => x._1).distinct(2)
    println("List of translated relations in source ontology "+ listOfRelationsInSourceOntology.count())
    listOfRelationsInSourceOntology.foreach(println(_))

    val relationSim = new RelationSimilarity()
    relationSim.GetRelationSimilarity(targetRelationsWithoutCodes.map(x=>x._1), listOfRelationsInSourceOntology)

    val translatedSourceOntologyClassTriples = translatedSourceOntology.subtract(translatedSourceOntologyRelationTriples)
    println("Translated classes of the source ontology " + translatedSourceOntologyClassTriples.count())
    translatedSourceOntologyClassTriples.foreach(println(_))


    println("======================================")
    println("|       Enrichment Phase      |")
    println("======================================")
    println("1. Hierarchical Enrichment:")
    val m = new HierarchicalEnrichment(sparkSession1)

    val monolingualTriplesForEnrichment: RDD[(String, String, String)] = m.GetTriplesForClassesToBeEnriched(translatedSourceOntologyClassTriples, targetClassesWithoutCodes, listOfMatchedClasses) //.cache()
    //    monolingualTriplesForEnrichment.foreach(println(_))
    val listOfNewClasses: RDD[String] = monolingualTriplesForEnrichment.map(x => x._1).union(monolingualTriplesForEnrichment.map(x => x._3)).filter(y => y != "Class").distinct(2)

    //    println("List of new classes that added to the target ontology is: " + listOfNewClasses.count())
    //    listOfNewClasses.foreach(println(_))
    val retrieveURIs = new RetrieveURIs(sparkSession1, sourceOntology)

    val triplesForHierarchicalEnrichmentWithURIs: RDD[(String, String, String)] = retrieveURIs.getTripleURIsForHierarchicalEnrichment(sourceClassesWithBestTranslation, monolingualTriplesForEnrichment)

    println("Triples for hierarchical enrichment with URIs: " + triplesForHierarchicalEnrichmentWithURIs.count())
    triplesForHierarchicalEnrichmentWithURIs.foreach(println(_))

    println("2. Relational Enrichment:")
    val listOfNewClassesBroadcasting: Broadcast[Map[String, Long]] = sparkSession1.sparkContext.broadcast(listOfNewClasses.zipWithIndex().collect().toMap)

    val RelEnrich = new RelationalEnrichment()

    val triplesRelationsForEnrichment = RelEnrich.GetTriplesForRelationsToBeEnriched(translatedSourceOntologyRelationTriples, listOfNewClassesBroadcasting)

    //    println("Triples for relations to be enriched: " + triplesRelationsForEnrichment.count())
    //    triplesRelationsForEnrichment.foreach(println(_))
    val newC = triplesRelationsForEnrichment.map(x => x._3).subtract(listOfNewClasses).distinct(2)

    //    println("List of new classes in triples for relations " + newC.count())
    //    newC.foreach(println(_))
    val triplesForRelationalEnrichmentWithURIs: RDD[(String, String, String)] = retrieveURIs.getTripleURIsForRelationalEnrichment(relationsWithTranslation, sourceClassesWithBestTranslation, triplesRelationsForEnrichment)

    println("Triples for relational enrichment with URIs: " + triplesForRelationalEnrichmentWithURIs.count())
    triplesForRelationalEnrichmentWithURIs.foreach(println(_))

    //    triplesForRelationalEnrichmentWithURIs.coalesce(1, shuffle = true).saveAsTextFile("Output/triplesForRelationalEnrichmentWithURIs(SEO-Conference)")
    val gCreate = new GraphCreating(sparkSession1)
    val triplesToBeEnrichedAsGraph: RDD[graph.Triple] = gCreate.createGraph(triplesForHierarchicalEnrichmentWithURIs).union(gCreate.createGraph(triplesForRelationalEnrichmentWithURIs))
    val enrichedTargetOntology = targetOntology.union(triplesToBeEnrichedAsGraph)

    //    triplesToBeEnrichedAsGraph.coalesce(1, shuffle = true).saveAsNTriplesFile("Output/TriplesToBeEnrichedGraph")
    println("Graph printing" + triplesToBeEnrichedAsGraph.count())
    triplesToBeEnrichedAsGraph.foreach(println(_)) //    enrichedTargetOntology.coalesce(1, shuffle = true).saveAsNTriplesFile("Output/EnrichedTargetOntology")
    println("Enriched Target Ontology: " + enrichedTargetOntology.count())
    enrichedTargetOntology.foreach(println(_))

    //    val triplesToBeEnrichedAsGraphWithForeignClassLabels = gCreate.createMultilingualGraphLabelsForClasses(triplesForHierarchicalEnrichmentWithURIs,sourceClassesWithBestTranslation)
    //    println("Foreign labels for classes")
    //    triplesToBeEnrichedAsGraphWithForeignClassLabels.foreach(println(_))
    //
    //    val triplesToBeEnrichedAsGraphWithForeignRelationsLabels = gCreate.createMultilingualGraphLabelsForRelations(triplesForRelationalEnrichmentWithURIs,relationsWithTranslation)
    //    println("Foreign labels for relations")
    //    triplesToBeEnrichedAsGraphWithForeignRelationsLabels.foreach(println(_))
    //    triplesToBeEnrichedAsGraphWithForeignClassLabels.union(triplesToBeEnrichedAsGraphWithForeignRelationsLabels).coalesce(1, shuffle = true).saveAsNTriplesFile("Output/MultilingualLabels")
    //################################## Quality Assessment ###################
    println("################################## Quality Assessment ###################")

    val enrichStats = new EnrichmentStatistics(sparkSession1)
    enrichStats.getEnrichmentStatistics(targetOntology, enrichedTargetOntology)

    val endTimeMillis = System.currentTimeMillis()
    val durationMinutes = (endTimeMillis - startTimeMillis) / (1000 * 60)
    println("runtime = " + durationMinutes + " minutes")

    sparkSession1.stop
  }
}
