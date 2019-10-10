

//import de.danielnaber.jwordsplitter.GermanWordSplitter
import edu.stanford.nlp.simple.{Document, Sentence}
import edu.stanford.nlp.tagger.maxent.MaxentTagger
import org.apache.jena.graph
import org.apache.jena.graph.Node
import org.apache.spark.rdd.RDD


class PreProcessing extends Serializable{
//  def RecreateSourceGermanOntologyWithClassLabels(ontologyTriples: RDD[graph.Triple]): RDD[(String, String, String)] = {
//    var classLabels: RDD[graph.Triple] = ontologyTriples.filter(x=>x.getPredicate.getLocalName == "label")
////    println("classes with labels "+classLabels.count())
////    classLabels.foreach(println(_))
//      var germanTagger: MaxentTagger = new MaxentTagger("edu/stanford/nlp/models/pos-tagger/german/german-fast.tagger") with Serializable
//
//    var ontologyWithSubjectClass: RDD[(Node, Node, Node)] = ontologyTriples.keyBy(_.getSubject).join(classLabels.keyBy(_.getSubject)).map(x=>(x._2._2.getObject,x._2._1.getPredicate,x._2._1.getObject)).filter(x=>x._2.getLocalName != "label")
////    println("After join")
////    ontologyWithSubjectClass.foreach(println(_))
//
//    var ontologyWithSubjectAndObjectClass: RDD[(String, String, String)] = ontologyWithSubjectClass.keyBy(_._3).join(classLabels.keyBy(_.getSubject)).map(x=>(this.germanPosTagForString(this.stringPreProcessing2(x._2._1._1.toString),germanTagger),x._2._1._2.getLocalName,this.germanPosTagForString(this.stringPreProcessing2(x._2._2.getObject.toString),germanTagger)))
//
//    ontologyWithSubjectAndObjectClass
//
//  }
  def RecreateOntologyWithLabels(ontologyTriples: RDD[graph.Triple]): RDD[(String, String, String)] = {
    println("Number of triples before mapping is "+ontologyTriples.count())
    val Labels: RDD[graph.Triple] = ontologyTriples.filter(x=>x.getPredicate.getLocalName == "label")
//    println("All labels "+Labels.count())
//    Labels.foreach(println(_))

//  val xx = ontologyTriples.keyBy(_.getSubject).leftOuterJoin(Labels.keyBy(_.getSubject)).map(x=>(x._2.))
//    //.map(x=>(x._2._2.getObject,x._2._1.getPredicate,x._2._1.getObject)).filter(x=>x._2.getLocalName != "label")
//  println("After left outer join" + xx.count())
//  xx.foreach(println(_))

    val ontologyWithSubjectLabel: RDD[(Node, Node, Node)] = ontologyTriples.keyBy(_.getSubject).join(Labels.keyBy(_.getSubject)).map(x=>(x._2._2.getObject,x._2._1.getPredicate,x._2._1.getObject)).filter(x=>x._2.getLocalName != "label")
    println("After join" + ontologyWithSubjectLabel.count())
    ontologyWithSubjectLabel.foreach(println(_))

    val triplesWitType: RDD[(String, String, String)] = ontologyWithSubjectLabel.filter(x=>x._2.getLocalName=="type").map(x=>(this.stringPreProcessing2(x._1.toString.toLowerCase),x._2.getLocalName,x._3.getLocalName))
  println("With type predicate only "+triplesWitType.count())
    triplesWitType.foreach(println(_))

    val ontologyWithSubjectAndObjectLabel: RDD[(String, String, String)] = ontologyWithSubjectLabel.keyBy(_._3).join(Labels.keyBy(_.getSubject)).map(x=>(this.stringPreProcessing2(x._2._1._1.toString).toLowerCase,x._2._1._2.getLocalName,this.stringPreProcessing2(x._2._2.getObject.toString).toLowerCase)).union(triplesWitType)

    ontologyWithSubjectAndObjectLabel

  }
  def stringPreProcessing(term: String): String = {
    //For SemSur and Edas and ekaw Datasets
    var splittedString: String = splitCamelCase(term).toLowerCase
    var preProcessedString: String = splittedString.replaceAll("""([\p{Punct}])\s*""", "").trim
//    var splittedString: String = splitCamelCase(preProcessedString).toLowerCase

    /*
    * for conference and cmt*/
//    var preProcessedString: String = term.replaceAll("""([\p{Punct}&&[^.]]|\b\p{IsLetter}{1,2}\b)\s*""", " ").trim.toLowerCase
//    var splittedString: String = splitCamelCase(preProcessedString).toLowerCase
//    splittedString
    preProcessedString
  }
  def stringPreProcessing2(term: String): String = {
    /*For SemSur Dataset
    var preProcessedString: String = term.replaceAll("""([\p{Punct}&&[^.]]|\b\p{IsLetter}{1,2}\b)\s*""", " ").trim
    var splittedString: String = splitCamelCase(preProcessedString).toLowerCase
     splittedString
    * */
    var preProcessedString: String = term.split("@").head.replace("\"", "")//.replaceAll("""([\p{Punct}&&[^.]]|\b\p{IsLetter}{1,2}\b)\s*""", " ").trim.toLowerCase
    var splittedString: String = splitCamelCase(preProcessedString).toLowerCase
    //    splittedString
    preProcessedString
  }
  def splitCamelCase(s: String): String = {
//    return s.replaceAll(
//      String.format("%s|%s|%s",
//        "(?<=[A-Z])(?=[A-Z][a-z])",
//        "(?<=[^A-Z])(?=[A-Z])",
//        "(?<=[A-Za-z])(?=[^A-Za-z])"
//      ),
//      " "
//    ).replaceAll("  ", " ").split(" ")
    return s.replaceAll(
      String.format("%s|%s|%s",
        "(?<=[A-Z])(?=[A-Z][a-z])",
        "(?<=[^A-Z])(?=[A-Z])",
        "(?<=[A-Za-z])(?=[^A-Za-z])"
      ),
      " "
    ).replaceAll(" ", " ")
  }
//  def germanWordSplitter(s: String):util.List[String]={
//    val splitter = new GermanWordSplitter(true)
//    val parts: util.List[String] = splitter.splitWord(s)
//    parts
//  }
  def ToCamel(s: String): String = {
    val split = s.split(" ")
    val tail = split.tail.map { x => x.head.toUpper + x.tail }
    split.head.capitalize+ tail.mkString
  }

  def getLastBitFromUrI(urI: String): String = {
//    urI.replaceFirst(".*/([^/?]+).*", "$1")
    urI.replaceFirst(".*/([^/?]+)", "$0")
  }
  def getURIWithoutLastString(urI: String): String = {
    urI.substring(0,urI.lastIndexOf("/")) + "/"

  }
  def getStringWithoutTags(str: Array[String]): String = {
    str.map(x=>x.split("_").head).mkString(" ")
  }

  def germanPosTag(sourceClassesWithoutURIs: Array[String], germanTagger: MaxentTagger): Array[String]={
    var sourceC: Array[String] = sourceClassesWithoutURIs.filter(x => x.split(" ").length == 1)
    var sourceC2 = sourceClassesWithoutURIs diff sourceC
//    println("####################### Subtraction results #######################")
//    sourceC2.foreach(println(_))
    var tags: Array[String] = sourceC2.map(x=>(germanTagger.tagString(x).split(" ")).filter(y=> y.contains("_ADJA") || y.contains("_NN")|| y.contains("_XY") || y.contains("_ADV")|| y.contains("_NE") || y.contains("_ADJD")).mkString(" "))
    var removeTags: Array[String] = tags.map(x=>this.getStringWithoutTags(x.split(" ")))
//    println("All Tags")
//    tags.foreach(println(_))
//    println("Removing Tags")
//    removeTags.foreach(println(_))
    var preprocessedSourceClasses: Array[String] = sourceC.union(removeTags)
//    println("All source classes after preprocessing")
//    preprocessedSourceClasses.foreach(println(_))
    preprocessedSourceClasses

  }
//  def germanPosTagForString(classLabel: String): String={
//    var tokens = classLabel.split(" ")
//    var strWithTags = germanTagger.tagTokenizedString(classLabel).split(" ").filter(y=> y.contains("_ADJA") || y.contains("_NN")|| y.contains("_XY") || y.contains("_ADV")|| y.contains("_NE") || y.contains("_ADJD"))//.mkString(" ")
//    var strWithoutTags = strWithTags.map(x=>x.split("_").head+" ").mkString
//    strWithoutTags
////    strWithTags
//
//  }
  def englishPosTagForString(classLabel: String): String={
  var englishTagger: MaxentTagger = new MaxentTagger("edu/stanford/nlp/models/pos-tagger/english-left3words/english-left3words-distsim.tagger")
    var tokens = classLabel.split(" ")
    var strWithTags = englishTagger.tagTokenizedString(classLabel).split(" ").filter(y=> !y.contains("_DT") && !y.contains("_IN") && !y.contains("_TO") && !y.contains("_WDT") && !y.contains("_WP") && !y.contains("_VBZ"))//.mkString(" ")
    var strWithoutTags = strWithTags.map(x=>x.split("_").head+" ").mkString
    this.stringPreProcessing(strWithoutTags.toLowerCase).replaceAll("\\s{2,}", " ").trim()
//    strWithTags

  }
  def sentenceLemmatization (sentence1: String):String={
    val doc = new Document(sentence1)
    var sent: Sentence = doc.sentences.get(0)
//    var lemmas = this.stringPreProcessing(sent.lemmas.toString.split(",").mkString).replaceAll(" +", " ")
    var lemmas = this.stringPreProcessing(sent.lemmas.toString.split(",").mkString).replaceAll(" +", " ")

    //    println("Lemmatization for "+ sent + " is "+ lemmas)
    lemmas
  }
}
