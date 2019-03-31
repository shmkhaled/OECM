

//import de.danielnaber.jwordsplitter.GermanWordSplitter
import edu.stanford.nlp.simple.{Document, Sentence}
import edu.stanford.nlp.tagger.maxent.MaxentTagger
import org.apache.jena.graph
import org.apache.jena.graph.Node
import org.apache.spark.rdd.RDD


class PreProcessing extends Serializable{
  def RecreateSourceGermanOntologyWithClassLabels(ontologyTriples: RDD[graph.Triple]): RDD[(String, String, String)] = {
    var classLabels: RDD[graph.Triple] = ontologyTriples.filter(x=>x.getPredicate.getLocalName == "label")
//    println("classes with labels "+classLabels.count())
//    classLabels.foreach(println(_))

    var ontologyWithSubjectClass: RDD[(Node, Node, Node)] = ontologyTriples.keyBy(_.getSubject).join(classLabels.keyBy(_.getSubject)).map(x=>(x._2._2.getObject,x._2._1.getPredicate,x._2._1.getObject)).filter(x=>x._2.getLocalName != "label")
//    println("After join")
//    ontologyWithSubjectClass.foreach(println(_))

    var ontologyWithSubjectAndObjectClass: RDD[(String, String, String)] = ontologyWithSubjectClass.keyBy(_._3).join(classLabels.keyBy(_.getSubject)).map(x=>(this.posTagForString(this.stringPreProcessing2(x._2._1._1.toString)),x._2._1._2.getLocalName,this.posTagForString(this.stringPreProcessing2(x._2._2.getObject.toString))))

    ontologyWithSubjectAndObjectClass

  }
  def RecreateOntologyWithClassLabels(ontologyTriples: RDD[graph.Triple]): RDD[(String, String, String)] = {
    var classLabels: RDD[graph.Triple] = ontologyTriples.filter(x=>x.getPredicate.getLocalName == "label")
    //    println("classes with labels "+classLabels.count())
    //    classLabels.foreach(println(_))

    var ontologyWithSubjectClass: RDD[(Node, Node, Node)] = ontologyTriples.keyBy(_.getSubject).join(classLabels.keyBy(_.getSubject)).map(x=>(x._2._2.getObject,x._2._1.getPredicate,x._2._1.getObject)).filter(x=>x._2.getLocalName != "label")
//    println("After join")
//    ontologyWithSubjectClass.foreach(println(_))
//
//    println("Classes only")
    var triplesWitTypeClass: RDD[(String, String, String)] = ontologyWithSubjectClass.filter(x=>x._2.getLocalName=="type").map(x=>(this.stringPreProcessing2(x._1.toString.toLowerCase),x._2.getLocalName,x._3.getLocalName))
//    triplesWitTypeClass.foreach(println(_))

    var ontologyWithSubjectAndObjectClass: RDD[(String, String, String)] = ontologyWithSubjectClass.keyBy(_._3).join(classLabels.keyBy(_.getSubject)).map(x=>(this.stringPreProcessing2(x._2._1._1.toString).toLowerCase,x._2._1._2.getLocalName,this.stringPreProcessing2(x._2._2.getObject.toString).toLowerCase)).union(triplesWitTypeClass)

    ontologyWithSubjectAndObjectClass

  }
  def stringPreProcessing(term: String): String = {
    //For SemSur and Edas and ekaw Datasets
    var preProcessedString: String = term.replaceAll("""([\p{Punct}&&[^.]]|\b\p{IsLetter}{1,2}\b)\s*""", "").trim
    var splittedString: String = splitCamelCase(preProcessedString).toLowerCase
     splittedString

    /*
    * for conference and cmt*/
//    var preProcessedString: String = term.replaceAll("""([\p{Punct}&&[^.]]|\b\p{IsLetter}{1,2}\b)\s*""", " ").trim.toLowerCase
//    var splittedString: String = splitCamelCase(preProcessedString).toLowerCase
    splittedString
//    preProcessedString
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

  def posTag(sourceClassesWithoutURIs: Array[String], germanTagger: MaxentTagger): Array[String]={
    var sourceC: Array[String] = sourceClassesWithoutURIs.filter(x => x.split(" ").length == 1)
    var sourceC2 = sourceClassesWithoutURIs diff sourceC
//    println("####################### Subtraction results #######################")
//    sourceC2.foreach(println(_))
    var tags: Array[String] = sourceC2.map(x=>(germanTagger.tagString(x).split(" ")).filter(y=> y.contains("_ADJA") || y.contains("_NN")|| y.contains("_XY") || y.contains("_ADV")|| y.contains("_NE")).mkString(" "))
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
  def posTagForString(classLabel: String): String={
    var tokens = classLabel.split(" ")
    var tagger = new MaxentTagger("src/main/resources/taggers/german-fast.tagger")
    var strWithTags = tagger.tagTokenizedString(classLabel).split(" ").filter(y=> y.contains("_ADJA") || y.contains("_NN")|| y.contains("_XY") || y.contains("_ADV")|| y.contains("_NE"))//.mkString(" ")
    var strWithoutTags = strWithTags.map(x=>x.split("_").head+" ").mkString
    strWithoutTags
//    strWithTags

  }
  def sentenceLemmatization (sentence1: String):String={
    val doc = new Document(sentence1)
    var sent: Sentence = doc.sentences.get(0)
    var lemmas = this.stringPreProcessing(sent.lemmas.toString.split(",").mkString).replaceAll(" +", " ")
//    println("Lemmatization for "+ sent + " is "+ lemmas)
    lemmas
  }
}
