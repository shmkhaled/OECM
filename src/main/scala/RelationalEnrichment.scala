import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
/*
* Created by Shimaa 23.10.2019
* */

class RelationalEnrichment {
  def GetTriplesForRelationsToBeEnriched (translatedSourceOntologyRelationTriples: RDD[(String, String, String)], listOfNewClassesBroadcasting: Broadcast[Map[String, Long]]): RDD[(String, String, String)]={
    val triplesForEnrichment: RDD[(String, String, String)] = translatedSourceOntologyRelationTriples.filter(x => listOfNewClassesBroadcasting.value.contains(x._3))
    triplesForEnrichment
  }

}
