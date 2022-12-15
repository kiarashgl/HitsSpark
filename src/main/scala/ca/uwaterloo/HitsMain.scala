package ca.uwaterloo

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}

object HitsMain {
  var inputGraph = ""
  var output = ""
  var iterations = 10
  val conf: SparkConf = new SparkConf().setAppName("Hits Algorithm")
  val sc = new SparkContext(conf)

  def main(argv: Array[String]): Unit = {
    val args = new HitsConf(argv)
    inputGraph = args.input()
    output = args.output()
    iterations = args.iterations()
    val titles = args.titles()

    val hits = new Hits(sc, inputGraph, output, iterations)
    if (titles.isEmpty)
      hits.run()
    else
      hits.getTitles(titles)
  }
}
