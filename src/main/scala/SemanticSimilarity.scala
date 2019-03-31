import edu.cmu.lti.lexical_db.NictWordNet
import edu.cmu.lti.ws4j.impl._
import edu.cmu.lti.ws4j.util.WS4JConfiguration
/*
* Created by Shimaa 27.march.2019
* */
class SemanticSimilarity extends Serializable{
  val db = new NictWordNet with Serializable
  val processing = new PreProcessing()
//  val rcs = Array(new HirstStOnge(db), new LeacockChodorow(db), new Lesk(db),  new WuPalmer(db), new Resnik(db), new JiangConrath(db), new Lin(db),new Path(db))
//
//  def getSemanticSimilarity(word1: String, word2: String): Unit = {
//    WS4JConfiguration.getInstance.setMFS(true)
//    for (rc <- rcs) {
//      val s = rc.calcRelatednessOfWords(word1, word2)
//      System.out.println(rc.getClass.getName + "\t" + s)
//    }
//  }
  def getPathSimilarity(word1: String, word2: String): Double={
    WS4JConfiguration.getInstance.setMFS(true)
    val path = new Path(db)
    var pathSim = path.calcRelatednessOfWords(word1,word2)
    if (pathSim>=1.0){
      pathSim = 1.0
//      println("path similarity between "+word1+" and "+word2+" = "+pathSim)
    }
  else if (pathSim == 0.0)
      pathSim = -1.0
    pathSim
  }
  def sentenceSimilarity(sentence1: String, sentence2: String): Double={
    var simScoure = 0.0
    var count = 0.0
    var s = 0.0
    for(word1 <- sentence1.split(" ")){
      var bestSroce: List[Double] = List()
      for (word2 <- sentence2.split(" "))
      {
        bestSroce ::= this.getPathSimilarity(word1,word2)
      }
      if (bestSroce.length > 1)
        s = max(bestSroce)
      else s = bestSroce.head
      if (s != -2){
        simScoure += s.asInstanceOf[Double]
        count += 1
      }
    }
    simScoure = Math.round((simScoure / count)*1000)/1000.0
//    println(sentence1 + " ##### "+ sentence2 +" = "+ simScoure)
    simScoure
  }
  def symmetricSentenceSimilarity(sentence1: String, sentence2: String): Double ={
    var sent1 = processing.sentenceLemmatization(sentence1)
    var sent2 = processing.sentenceLemmatization(sentence2)
    var sim = (sentenceSimilarity(sent1,sent2) + sentenceSimilarity(sent2,sent1))/2
    sim
  }
  def max(lst: List[Double]): Double={
    var maxValue = lst.max
    if (maxValue == lst.head && maxValue == lst.last)
      maxValue = -2
    maxValue
  }

}
