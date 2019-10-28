import org.apache.jena.graph
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/*
* Created by Shimaa Ibrahim 28 October 2019
* */

class EnrichmentStatistics(sparkSession: SparkSession) {
  def getEnrichmentStatistics(targetOntology: RDD[graph.Triple], enrichedTargetOntology: RDD[graph.Triple])={
    val ontoStat = new OntologyStatistics(sparkSession)
    val QA = new QualityAssessment(sparkSession)
    val numOfClassesBeforeEnrichment = ontoStat.GetNumberOfClasses(targetOntology)
    val numOfClassesAfterEnrichment = ontoStat.GetNumberOfClasses(enrichedTargetOntology)
    val enrichmentRatioForClasses = ((numOfClassesAfterEnrichment-numOfClassesBeforeEnrichment)/numOfClassesBeforeEnrichment)*100
    println("Number of classes before enrichment = "+ numOfClassesBeforeEnrichment+" after enrichment = "+numOfClassesAfterEnrichment+ " i.e. enrichment by "+ enrichmentRatioForClasses.round + " %")

    val numOfRelationsBeforeEnrichment = ontoStat.GetNumberOfRelations(targetOntology)
    val numOfRelationsAfterEnrichment = ontoStat.GetNumberOfRelations(enrichedTargetOntology)
    val enrichmentRatioForRelations = ((numOfRelationsAfterEnrichment-numOfRelationsBeforeEnrichment)/numOfRelationsBeforeEnrichment)*100
    println("Number of relations before enrichment = "+numOfRelationsBeforeEnrichment+" after enrichment = "+numOfRelationsAfterEnrichment+ " i.e. enrichment by "+ enrichmentRatioForRelations.round + " %")

    val numOfTriplesBeforeEnrichment = targetOntology.count()
    val numOfTriplesAfterEnrichment = enrichedTargetOntology.count()
    val triplesRatio = ((numOfTriplesAfterEnrichment-numOfTriplesBeforeEnrichment)/numOfTriplesBeforeEnrichment)*100
    println("Number of triples before enrichment = "+numOfTriplesBeforeEnrichment+" after enrichment = "+numOfTriplesAfterEnrichment+ " i.e. increased by "+ triplesRatio.round + " %")

    val relationshipRichnessBeforeEnrichment = QA.RelationshipRichness(targetOntology)
    val relationshipRichnessAfterEnrichment = QA.RelationshipRichness(enrichedTargetOntology)
    val enrichmentRatioForRelationshipRichness = ((relationshipRichnessAfterEnrichment-relationshipRichnessBeforeEnrichment)/relationshipRichnessBeforeEnrichment)*100
    println("Relationship richness before enrichment = "+ontoStat.Round(relationshipRichnessBeforeEnrichment)+" after enrichment = "+ontoStat.Round(relationshipRichnessAfterEnrichment)+ " i.e. increased by "+ ontoStat.Round(enrichmentRatioForRelationshipRichness)+ " %")

    val inheritanceRichnessBeforeEnrichment = QA.InheritanceRichness(targetOntology)
    val inheritanceRichnessAfterEnrichment = QA.InheritanceRichness(enrichedTargetOntology)
    val inheritanceRichnessRatio = ((inheritanceRichnessAfterEnrichment-inheritanceRichnessBeforeEnrichment)/inheritanceRichnessBeforeEnrichment)*100
    println("Inheritance richness before enrichment = "+ontoStat.Round(inheritanceRichnessBeforeEnrichment)+" after enrichment = "+ontoStat.Round(inheritanceRichnessAfterEnrichment)+ " i.e. increased by "+ ontoStat.Round(inheritanceRichnessRatio)+ " %")

    val schemaCompletenessBeforeEnrichment = QA.SchemaCompleteness(targetOntology)
    val schemaCompletenessAfterEnrichment = QA.SchemaCompleteness(enrichedTargetOntology)
    val schemaCompletenessRatio = ((schemaCompletenessAfterEnrichment-schemaCompletenessBeforeEnrichment)/schemaCompletenessBeforeEnrichment)*100
    println("Schema Completeness before enrichment = "+ontoStat.Round(schemaCompletenessBeforeEnrichment)+" after enrichment = "+ontoStat.Round(schemaCompletenessAfterEnrichment)+ " i.e. increased by "+ ontoStat.Round(schemaCompletenessRatio)+ " %")

    val interlinkingCompBeforeEnrichment = QA.InterlinkingCompleteness(targetOntology)
    val interlinkingCompAfterEnrichment = QA.InterlinkingCompleteness(enrichedTargetOntology)
    val interlinkingCompRatio = ((interlinkingCompAfterEnrichment-interlinkingCompBeforeEnrichment)/interlinkingCompBeforeEnrichment)*100
    println("Interlinking Completeness before enrichment = "+ontoStat.Round(interlinkingCompBeforeEnrichment)+" after enrichment = "+ontoStat.Round(interlinkingCompAfterEnrichment)+ " i.e. increased by "+ ontoStat.Round(interlinkingCompRatio)+ " %")

    val dereferenceableUrisBeforeEnrichment = QA.DereferenceableUris(targetOntology)
    val dereferenceableUrisAfterEnrichment = QA.DereferenceableUris(enrichedTargetOntology)
    println("Dereferenceable Uris before enrichment = "+ontoStat.Round(dereferenceableUrisBeforeEnrichment)+" after enrichment = "+ontoStat.Round(dereferenceableUrisAfterEnrichment))

    val coverageDetailBeforeEnrichment = QA.CoverageDetail(targetOntology)
    val coverageDetailAfterEnrichment = QA.CoverageDetail(enrichedTargetOntology)
    println("Coverage Detail before enrichment = "+ontoStat.Round(coverageDetailBeforeEnrichment)+" after enrichment = "+ontoStat.Round(coverageDetailAfterEnrichment))

    val amountOfTriplesBeforeEnrichment = QA.AmountOfTriples(targetOntology)
    val amountOfTriplesAfterEnrichment = QA.AmountOfTriples(enrichedTargetOntology)
    println("Amount of Triples before enrichment = "+ontoStat.Round(amountOfTriplesBeforeEnrichment)+" after enrichment = "+ontoStat.Round(amountOfTriplesAfterEnrichment))

    val noHashUrisBeforeEnrichment = QA.NoHashUris(targetOntology)
    val noHashUrisAfterEnrichment = QA.NoHashUris(enrichedTargetOntology)
    println("No Hash Uris before enrichment = "+ontoStat.Round(noHashUrisBeforeEnrichment)+" after enrichment = "+ontoStat.Round(noHashUrisAfterEnrichment))

    val labeledResourcesEnrichment = QA.LabeledResources(targetOntology)
    val labeledResourcesAfterEnrichment = QA.LabeledResources(enrichedTargetOntology)
    println("Labeled Resources before enrichment = "+ontoStat.Round(labeledResourcesEnrichment)+" after enrichment = "+ontoStat.Round(labeledResourcesAfterEnrichment))

  }

}
