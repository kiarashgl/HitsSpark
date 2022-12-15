package ca.uwaterloo

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class Hits(sc: SparkContext, val inputGraph: String, val output: String, val iterations: Int) {

  def createNodes(): RDD[Node] = {
    val outEdges = graph.map(_.split("\\s+"))
      .map(item => (item.head.toInt, item.drop(1).map(_.toInt).toSeq))

    val inEdges = outEdges.flatMap { case (node, adjList) =>
      adjList.map((_, node))
    }.groupByKey()
      .map { case (node, iterable) => (node, iterable.toSeq) }

    val nodes = inEdges.fullOuterJoin(outEdges)
      .mapValues(pair => (pair._1.getOrElse(Seq[Int]()).to[collection.immutable.Seq], pair._2.getOrElse(Seq[Int]()).to[collection.immutable.Seq]))
      .map { case (id, (inEdges, outEdges)) => new Node(id, inEdges, outEdges) }
    nodes
  }

  def rootSumOfSquares(list: RDD[Double]): Double = {
    math.sqrt(list.map(item => item * item).sum)
  }

  def updateAuthorities(): Unit = {
    val outEdges = nodes.map(node => (node.id, node.outEdges))
    val hubs = nodeAttributes.map(node => (node.id, node.hub))
    var newAuthorities = outEdges.join(hubs)
      .flatMap(item => item._2._1.map((_, item._2._2)))
      .reduceByKey(_ + _)
    val normalizingFactor = rootSumOfSquares(newAuthorities.map(_._2))
    newAuthorities = newAuthorities.mapValues(_ / normalizingFactor)
    nodeAttributes = nodeAttributes.map(item => (item.id, item))
      .leftOuterJoin(newAuthorities)
      .mapValues(pair => (pair._1, pair._2.getOrElse(0.0)))
      .map { case (id, (node, authority)) =>
        node.copy(auth = authority)
      }
  }

  def updateHubs(): Unit = {
    val inEdges = nodes.map(node => (node.id, node.inEdges))
    val authorities = nodeAttributes.map(node => (node.id, node.auth))
    var newHubs = inEdges.join(authorities)
      .flatMap(item => item._2._1.map((_, item._2._2)))
      .reduceByKey(_ + _)
    val normalizingFactor = rootSumOfSquares(newHubs.map(_._2))
    newHubs = newHubs.mapValues(_ / normalizingFactor)
    nodeAttributes = nodeAttributes.map(item => (item.id, item))
      .leftOuterJoin(newHubs)
      .mapValues(pair => (pair._1, pair._2.getOrElse(0.0)))
      .map { case (id, (node, hub)) =>
        node.copy(hub = hub)
      }
  }

  val graph: RDD[String] = sc.textFile(inputGraph)
  val nodes: RDD[Node] = createNodes()
  var nodeAttributes: RDD[NodeAttribute] = nodes.map(node => NodeAttribute(node.id, 1.0, 1.0))

  def run(): Unit = {

    for (i <- 1 to iterations) {
      printf("Iteration %d\n", i)
      updateAuthorities()
      updateHubs()
      val currentOutput = output + i.toString
      val outputDir = new Path(currentOutput)
      FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

      nodeAttributes.map(node => (node.id, node.hub, node.auth)).saveAsTextFile(currentOutput)
    }}

  def getTitles(titlesPath: String) : Unit ={
    val titles = sc.textFile(titlesPath).map(line => {
      val splitLine = line.split("\\t")
      val id = splitLine(0).toInt
      val title = splitLine(1)
      (id, title)
    })
    val scores = sc.textFile(output + iterations.toString)
      .map(line => {
        val splitLine = line.drop(1).dropRight(1).split(",")
        val id = splitLine(0).toInt
        val hub = splitLine(1).toDouble
        val auth = splitLine(2).toDouble
        (id, (hub, auth))
      })
    val topHubs = scores.sortBy(_._2._1, ascending = false).zipWithIndex().filter(_._2 < 100).map(_._1)
    val topAuths = scores.sortBy(_._2._2, ascending = false).zipWithIndex().filter(_._2 < 100).map(_._1)
    val topHubNames = topHubs.join(titles).map(item => item._1.toString + " " + item._2._2)
    val topAuthNames = topAuths.join(titles).map(item => item._1.toString + " " + item._2._2)
    topHubNames.coalesce(1).saveAsTextFile("topHubs")
    topAuthNames.coalesce(1).saveAsTextFile("topAuths")
  }
}
