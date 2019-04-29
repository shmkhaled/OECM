import edu.cmu.lti.lexical_db.NictWordNet
import edu.cmu.lti.ws4j.impl.Path
import edu.cmu.lti.ws4j.util.WS4JConfiguration
import info.debatty.java.stringsimilarity.Jaccard
/*
* Created by Shimaa 6.11.2018
* */

class GetSimilarity extends Serializable{
  val db = new NictWordNet with Serializable
  val processing = new PreProcessing()
  def getSimilarity(sentence1: String, sentence2: String): Double={
    var sent1 = processing.sentenceLemmatization(sentence1)
    var sent2 = processing.sentenceLemmatization(sentence2)
    var sim = this.getJaccardStringSimilarity(sent1, sent2)
//    var jaccardSim = 1.0
    if (sim != 1)
      sim = (this.sentenceSimilarity(sent1,sent2)+this.sentenceSimilarity(sent2,sent1))/2
//      sim = this.symmetricSentenceSimilarity(sent1,sent2)
//    else if (sentence1.split(" ").length > 1 && sentence2.split(" ").length >1 && sim == 1)
//      jaccardSim = this.getJaccardStringSimilarity(sentence1,sentence2)
//    else if (sentence1.split(" ").length == 1 && sentence2.split(" ").length >1 && sim == 1)
//      jaccardSim = this.getJaccardStringSimilarity(sentence1,sentence2)
//    else if (sentence1.split(" ").length > 1 && sentence2.split(" ").length == 1 && sim == 1)
//      jaccardSim = this.getJaccardStringSimilarity(sentence1,sentence2)
//    if (jaccardSim != 1.0)
//      sim = 0.5

    sim
  }
  def getJaccardStringSimilarity(s1: String, s2: String): Double={
    val j = new Jaccard(3)
    var jaccardSim = j.similarity(s1, s2)
    jaccardSim
  }
//  def getStringSimilarity(s1: String, s2: String): Double={
//    //    val cos = new Cosine(2)
//    //    var cosSim = cos.similarity(s1, s2)
//    ////    System.out.println("Cosine similarity is "+cosSim)
//    //  import info.debatty.java.stringsimilarity.JaroWinkler
//    //  val jw = new JaroWinkler
//    //  var jarSim = jw.similarity(s1, s2)
//    //
//    //    val l = new NormalizedLevenshtein()
//    //    var levenshteinSim = l.distance(s1, s2)
//    //    System.out.println("Normalized Levenshtein similarity is "+levenshteinSim)
//
//    val j = new Jaccard(3)
//    var jaccardSim = j.similarity(s1, s2)
//    //    System.out.println("Jaccard similarity is "+jaccardSim)
//
//    //    import info.debatty.java.stringsimilarity.NGram
//    //    val trigram = new NGram(3)
//    //    var trigramSim = trigram.distance(s1, s2)
//    //    System.out.println("trigram similarity is "+trigram.distance(s1, s2))
//
//    jaccardSim
//
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
//      if (s != -2){
//        simScoure += s.asInstanceOf[Double]
//        count += 1
//      }
      simScoure += s.asInstanceOf[Double]
      count += 1
    }
    simScoure = Math.round((simScoure / count)*1000)/1000.0
    //    println(sentence1 + " ##### "+ sentence2 +" = "+ simScoure)
    simScoure
  }
  def symmetricSentenceSimilarity(sentence1: String, sentence2: String): Double ={
//    var sent1 = processing.sentenceLemmatization(sentence1)
//    var sent2 = processing.sentenceLemmatization(sentence2)
    var sim = (sentenceSimilarity(sentence1,sentence2) + sentenceSimilarity(sentence2,sentence1))/2
//    var sim = (sentenceSimilarity(sent1,sent2) + sentenceSimilarity(sent2,sent1))/2
    sim
  }
  def max(lst: List[Double]): Double={
    var maxValue = lst.max
    if (maxValue == lst.head && maxValue == lst.last)
//      maxValue = -2
      maxValue = 0
    maxValue
  }
}
