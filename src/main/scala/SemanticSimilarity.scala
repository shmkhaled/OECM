import edu.cmu.lti.lexical_db.NictWordNet
import edu.cmu.lti.ws4j.impl._
import edu.cmu.lti.ws4j.util.WS4JConfiguration
/*
* Created by Shimaa 27.march.2019
* */
class SemanticSimilarity extends Serializable{
  val db = new NictWordNet with Serializable
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
    var s = path.calcRelatednessOfWords(word1,word2)
    if (s>=1.0){
      s = 1.0
      println("path similarity between "+word1+" and "+word2+" = "+s)
    }
    s
  }
}
