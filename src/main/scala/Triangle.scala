/**
  * Created by wangyan on 16-7-23.
  */

import org.apache.spark.graphx.Graph

import scala.reflect.ClassTag
import org.apache.spark._
import org.apache.spark.graphx.PartitionStrategy.RandomVertexCut


object Triangle {
  /**
    * Compute the number of triangles
    * return a graph with vertex value representing the local clustering coefficient of that vertex
    *
    * @param p probability for sampling
    * @param graph the graph for which to count triangles
    *
    * @return approximated number of triangles
    *
    */
  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD,ED], p: Double): Double = {
    // Remove redundant edges
    val g = graph.groupEdges((a, b) => a).cache()

    // sample graph
    scala.util.Random.setSeed(745623)
    val subgraph = g.subgraph(epred = (edge) => scala.util.Random.nextFloat() <= p)

    // count triangles in subgraph
    val triangleCount = subgraph.triangleCount()
    val verts = triangleCount.vertices

    // approximation
    var res: Double = 0
    verts.collect().foreach { case (vid, count) =>
      res += count / (p * p * p)
    }
    res / 3
  }

  def main(args: Array[String]): Unit ={
    val conf = new SparkConf().setAppName("test app").setMaster("local")
    val sc = new SparkContext(conf)

    // build a triangle graph with duplicated edges
    val edges = Array( 0L->1L, 1L->2L, 2L->0L )
    val rawEdges = sc.parallelize(edges ++ edges, 2)
    val graph = Graph.fromEdgeTuples(rawEdges, true, uniqueEdges = Some(RandomVertexCut)).cache()

    // output results
    val count = run(graph, 1.0)
    println(count)
    sc.stop()
  }
}