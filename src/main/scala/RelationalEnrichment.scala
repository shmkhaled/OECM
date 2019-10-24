import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

/*
* Created by Shimaa 23.10.2019
* */
class RelationalEnrichment {
  def GetTriplesForRelationsToBeEnriched(translatedSourceOntologyRelationTriples: RDD[(String, String, String)], listOfNewClassesBroadcasting: Broadcast[Map[String, Long]]): RDD[(String, String, String)] = {

    val relationsWithClasses: RDD[(String, String, String)] = translatedSourceOntologyRelationTriples.filter(x => listOfNewClassesBroadcasting.value.contains(x._3))

    val triplesForEnrichment = relationsWithClasses.keyBy(_._1).leftOuterJoin(translatedSourceOntologyRelationTriples.keyBy(_._1)).map { case (rel1, ((rel2, pre1, class1), Some((rel3, pre2, class2)))) => (rel1, pre2, class2) }.distinct(2)

    triplesForEnrichment
  }

}
