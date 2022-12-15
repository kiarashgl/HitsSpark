package ca.uwaterloo

case class Node(id: Int, inEdges: Seq[Int], outEdges: Seq[Int])

case class NodeAttribute(id: Int, hub: Double, auth: Double)