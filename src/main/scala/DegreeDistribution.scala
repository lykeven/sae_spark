/**
  * Created by wangyan on 16-7-29.
  * */

import org.apache.spark.graphx.PartitionStrategy.RandomVertexCut
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

object DegreeDistribution {

    def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]):RDD[(Int, Double)] ={
      val g = graph.groupEdges((a, b) => a).cache()
      val n = g.numVertices
      val degree = g.outDegrees.mapValues(outDegree => (outDegree ->1.0)).
        values.reduceByKey(_ + _).mapValues(count => count / n).sortByKey()
      return degree
    }

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("DD").setMaster("local")
    val sc = new SparkContext(conf)
    val edges = Array((0L, 1L), (1L, 2L),(2L,0L),(2L,3L),(3L,4L),(4L,5L),(5L,3L))
    val rawEdges = sc.parallelize(edges ++ edges)
    val graph = Graph.fromEdgeTuples(rawEdges, true, uniqueEdges = Some(RandomVertexCut)).cache()
    val degree = run(graph)
    degree.collect().foreach { case(de,percent)=>
      println(de + " : "+ percent)
    }
  }


}
