package ca.uwaterloo

import org.rogach.scallop.{ScallopConf, ScallopOption}

class HitsConf(args: Seq[String]) extends ScallopConf(args) {
  val input: ScallopOption[String] = opt[String](descr = "input path", required = true)
  val output: ScallopOption[String] = opt[String](descr = "output path", required = true)
  val iterations: ScallopOption[Int] = opt[Int](descr = "number of iterations", required = true, default = Some(20))
  val titles: ScallopOption[String] = opt[String](descr = "title of pages", required = false)
  mainOptions = Seq(input, output, iterations, titles)
  verify()
}