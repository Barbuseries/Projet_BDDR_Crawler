import java.io.{FileInputStream, FileOutputStream, ObjectInputStream, ObjectOutputStream}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.jsoup.nodes.Document
import org.jsoup.{HttpStatusException, Jsoup}

import scala.collection.JavaConversions._
import scala.collection.concurrent.TrieMap
import scala.collection.mutable

object Main {
  val baseUrl = "http://www.d20pfsrd.com/bestiary/monster-listings"
  val filename = "crawl_result.dat"

  type WebMap = Broadcast[TrieMap[String, Int]]

  def getDoc(url: String): Document = {
    try {
      val doc = Jsoup.connect(url).get()

      return doc
    }
    catch {
      case hse: HttpStatusException => { println(s"invalid url: $url"); return null }
      case e: Exception => { println(e); return null }
    }
  }

  def crawlCreature(url: String, pagesVisited: WebMap): Creature = {
    if (pagesVisited.value.contains(url)) return null
    else pagesVisited.value.put(url, 1)

    val doc = getDoc(url)
    if (doc == null) return null

    val elems = doc.select("[class=spell]")

    val spells = (for (e <- elems) yield e.text).distinct.map(s => s.split(" ").map(_.capitalize).mkString(" ")).filter(_ != "")
    if (spells.length == 0) return null


    // Title can either be:
    // <Category>, <Name> – dXXXX
    // or
    // <Name> - dXXX
    val pattern = "(.*, )?(.*) – .*".r
    var pattern(category, name) = doc.select("title")(0).text

    if (category == null)
      category = name
    else
      category = category.substring(0, category.length - 2)

    if (name.contains("3pp") ||
        name.contains("Advanced")) return null

    println(url)
    return new Creature(name, category, spells)
  }

  def crawlAllCreaturesFromCategory(url: String, pagesVisited: WebMap): mutable.Buffer[Creature] = {
    val completeUrl = url

    val doc = getDoc(url)
    if (doc == null) return new mutable.ListBuffer[Creature]

    val elems = doc.select(s"a[href~=$completeUrl.]")

    var links = for ( e <- elems) yield e.attr("href")

    // A creature may be the only one in a category.
    // In that case, the page is directly it's description.
    if (links.length == 0) {
      links += completeUrl
    }
    else {
      pagesVisited.value.put(url, 1)
    }

    val creatures = for ( l <- links) yield crawlCreature(l, pagesVisited)

    val isNotNull: Creature => Boolean = _ != null

    return creatures.filter(isNotNull)
  }

  def crawlDeeper(url: String): mutable.Buffer[String] = {
    val doc = getDoc(url)
    if (doc == null) return new mutable.ListBuffer[String]

    val elems = doc.select(s"a[href~=$url.]")

    val items = for ( e <- elems) yield e.attr("href")

    return items.distinct
  }

  def crawlCategoriesFromType(creatureType: String): mutable.Buffer[String] = {
    val completeUrl = s"$creatureType"

    return crawlDeeper(completeUrl)
  }

  def crawlTypesFromBaseUrl(): mutable.Buffer[String] = {
    val completeUrl = s"$baseUrl/"

    return crawlDeeper(completeUrl)
  }

  def save[T](thing: T, filename: String): Unit = {
    println("Saving...")
    val os = new ObjectOutputStream(new FileOutputStream(filename))
    os.writeObject(thing)
    os.close()
    println("Done.")
  }

  def load[T](filename: String): T = {
    try {
      val is = new ObjectInputStream(new FileInputStream(filename))
      val obj = is.readObject().asInstanceOf[T]
      is.close()

      return obj
    }
    catch{
      case _: Throwable => null.asInstanceOf[T]
    }
  }

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("toto")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val pagesVisited = sc.broadcast(TrieMap.empty[String, Int])

    var oldCrawl = load[Array[Creature]](filename)
    var allCreatures: RDD[Creature] = sc.emptyRDD[Creature]

    var result: Array[(String, List[String])] = null
    val needsCrawl = (oldCrawl == null)

    if (needsCrawl) {
      val allCreatureTypes = sc.makeRDD(crawlTypesFromBaseUrl())


      allCreatures = allCreatureTypes.flatMap(t => crawlCategoriesFromType(t) // get all categories
                                     .flatMap(cat => crawlAllCreaturesFromCategory(cat, pagesVisited) // get all creatures
                                       .groupBy(_.name).map(_._2.head))) // get creatures uniquely (e.g, url ~= XXX, XXX-2)



      val temp = allCreatures.collect()
      save(temp, filename)

      oldCrawl = load[Array[Creature]](filename)
    }

    allCreatures = sc.makeRDD(oldCrawl)

    result = allCreatures.flatMap(c => c.spellList.map(s => (s, List(c.name)))) // map by (formalized) spell name
                         .reduceByKey((a, b) => a ++ b) // group by spell name
                         .sortBy(_._1).collect()

    println(s"Spell count: ${result.length}")
    for (r <- result) {
        println(r._1, r._2)
    }

    if (needsCrawl) println(s"Total pages visited: ${pagesVisited.value.size}")
  }
}