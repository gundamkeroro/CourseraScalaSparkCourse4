package wikipedia

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

case class WikipediaArticle(title: String, text: String)

object WikipediaRanking {

  val langs = List(
    "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
    "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")

  val conf: SparkConf = new SparkConf().setMaster("local").setAppName("Wiki")
  val sc: SparkContext = new SparkContext(conf)
  val wikiRdd: RDD[WikipediaArticle] = sc.textFile(WikipediaData.filePath).map(s => WikipediaData.parse(s))

  /** Returns the number of articles on which the language `lang` occurs.
    */
  def containsLanguage(wa: String, lang: String) = wa.split(" ").contains(lang)

  def occurrencesOfLang(lang: String, rdd: RDD[WikipediaArticle]): Int = {
    val listRDD = rdd.map(ele => (ele.title, containsLanguage(ele.text, lang)))
      .map(ele => (ele._1, ele._2.compareTo(false)))
    val result = listRDD.aggregate(0)((acc: Int, listRDD) => acc + listRDD._2,
      (acc1: Int, acc2: Int) => acc1 + acc2)
    result
  }


  /* (1) Use `occurrencesOfLang` to compute the ranking of the languages
   *     (`val langs`) by determining the number of Wikipedia articles that
   *     mention each language at least once.
   */
  def rankLangs(langs: List[String], rdd: RDD[WikipediaArticle]): List[(String, Int)] = {
    val result = langs.map(lang => (lang, occurrencesOfLang(lang, rdd))).sortBy(-_._2)
    result
  }

  /* Compute an inverted index of the set of articles, mapping each language
   * to the Wikipedia pages in which it occurs.
   */

  def makeIndex(langs: List[String], rdd: RDD[WikipediaArticle]): RDD[(String, Iterable[WikipediaArticle])] = {
    val rddIndex = rdd.flatMap(wa => for (l <- langs; if (containsLanguage(wa.text, l))) yield (l, wa)).groupByKey()
    rddIndex
  }

  /* (2) Compute the language ranking again, but now using the inverted index.
   */
  def rankLangsUsingIndex(index: RDD[(String, Iterable[WikipediaArticle])]): List[(String, Int)] = {
    val result = index.map(ele => (ele._1, ele._2.toList.length)).collect().toList
    result.sortBy(-_._2)
  }

  /* (3) Use `reduceByKey` so that the computation of the index and the ranking is combined.
   */
  def rankLangsReduceByKey(langs: List[String], rdd: RDD[WikipediaArticle]): List[(String, Int)] = {
    val toListRdd = rdd.flatMap(wa => for (l <- langs; if (containsLanguage(wa.text, l))) yield (l, 1))
    val result = toListRdd.reduceByKey(_ + _).sortBy(-_._2).collect().toList
    result
  }

  def main(args: Array[String]) {

    /* Languages ranked according to (1) */
    val langsRanked: List[(String, Int)] = timed("Part 1: naive ranking", rankLangs(langs, wikiRdd))

    /* An inverted index mapping languages to wikipedia pages on which they appear */
    def index: RDD[(String, Iterable[WikipediaArticle])] = makeIndex(langs, wikiRdd)

    /* Languages ranked according to (2), using the inverted index */
    val langsRanked2: List[(String, Int)] = timed("Part 2: ranking using inverted index", rankLangsUsingIndex(index))

    /* Languages ranked according to (3) */
    val langsRanked3: List[(String, Int)] = timed("Part 3: ranking using reduceByKey", rankLangsReduceByKey(langs, wikiRdd))

    /* Output the speed of each ranking */
    println(timing)
    sc.stop()
  }

  val timing = new StringBuffer

  def timed[T](label: String, code: => T): T = {
    val start = System.currentTimeMillis()
    val result = code
    val stop = System.currentTimeMillis()
    timing.append(s"Processing $label took ${stop - start} ms.\n")
    result
  }
}
