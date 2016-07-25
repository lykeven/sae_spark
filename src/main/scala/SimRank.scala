/**
  * Created by wangyan on 16-7-23.
  */

import org.apache.spark._

import scala.collection.mutable.ArrayBuffer
import SparkContext._
import org.apache.spark.graphx.PartitionStrategy.RandomVertexCut
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

object SimRank {

  //change simrank values of (i,i) to 1.0
  def change(s: (String, Double)): (String, Double) = {
    val line = s._1.split('_')
    if (line(0) == line(1)) (s._1, 1.0)
    else s
  }

  def change2(s: ((Long, Long), Double)): ((Long, Long), Double) = {
    if (s._1._1 == s._1._2) (s._1, 1.0)
    else s
  }

  def simRankByGraphx[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): RDD[((Long, Long), Double)] = {
    val g = graph.groupEdges((a, b) => (a)).cache()

    val inDegree = g.inDegrees
    val inDegreePair = inDegree.cartesian(inDegree).map { case ((vid1, degree1), (vid2, degree2)) =>
      ((vid1, vid2), degree1 * degree2)

    }.cache()

    val nodeList = g.vertices
    var simrank = nodeList.cartesian(nodeList).map { case ((vid1, _), (vid2, _)) =>
      if (vid1 == vid2) ((vid1, vid2), 1.0)
      else ((vid1, vid2), 0.0)
    }.cache()

    val outEdges = g.collectNeighborIds(EdgeDirection.Out)
    val graphPair = outEdges.cartesian(outEdges).flatMap { case ((vid1, nbr1), (vid2, nbr2)) =>
      var pair = ArrayBuffer[(Long, Long)]()
      for (x <- nbr1) {
        for (y <- nbr2) {
          pair.append((x, y))
        }
      }
      pair.map { f => ((vid1, vid2), f) }
    }.groupByKey().cache()

    val C = 0.8
    val iters = 10
    //iteratively update simrank values
    for (i <- 1 to iters) {
      val con = graphPair.join(simrank).values.flatMap { case (urls, sim) =>
        urls.map(url => (url, sim))
      }
      //update simrank values with definition
      simrank = con.reduceByKey(_ + _).join(inDegreePair).mapValues { s =>
        if (s._2 > 10e-10) s._1 / s._2 * C
        else 0.0
      }
      simrank = simrank.map(s => change2(s))
      simrank.foreach(println _)
    }
    return simrank
  }


  def simRank() {

    val C = 0.8
    val iters = 10
    val conf = new SparkConf().setAppName("SimRankTest")
    val sc = new SparkContext(conf)

    val graphLine = sc.textFile("/home/wangyan/IdeaProjects/test2/in/graphLine.txt")
    val outEdges = graphLine.map { s => (s.split(':')(0), s.split(':')(1)) }.cache()
    //node pairs and their outdegree node pairs, which combine with each outdegree nodes of these two nodes
    val graph = outEdges.cartesian(outEdges).flatMap { s =>
      val line1 = s._1._2.split(',')
      val line2 = s._2._2.split(',')
      val list = ArrayBuffer[String]()
      for (x <- line1) {
        for (y <- line2) {
          list += x + '_' + y
        }
      }
      list.map { f => (s._1._1 + '_' + s._2._1, f) }
    }.groupByKey().cache()

    val inDegree = outEdges.map { s => (s._1, s._2.split(',').length) }
    //indegree product between two nodes
    val inDegreePair = inDegree.cartesian(inDegree).map { s => (s._1._1 + '_' + s._2._1, s._1._2 * s._2._2) }.cache()

    val nodeList = outEdges.keys
    // initialize simrank values
    var simrank = nodeList.cartesian(nodeList).map { s =>
      if (s._1 == s._2) (s._1 + '_' + s._2, 1.0)
      else (s._1 + '_' + s._2, 0.0)
    }.cache()

    //iteratively update simrank values
    for (i <- 1 to iters) {
      val con = graph.join(simrank).values.flatMap { case (urls, sim) => urls.map(url => (url, sim)) }
      //update simrank values with definition
      simrank = con.reduceByKey(_ + _).join(inDegreePair).mapValues { s =>
        if (s._2 > 10e-10) s._1 / s._2 * C
        else 0.0
      }
      simrank = simrank.map(s => change(s))
      simrank.foreach(println _)
    }
    //simrank.saveAsTextFile("/home/wangyan/IdeaProjects/test2/out/simrank")
    sc.stop()
  }

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("learn").setMaster("local")
    val sc = new SparkContext(conf)

    val edges = Array((0L, 1L), (0L, 3L),(1L, 2L), (2L, 1L), (2L, 3L) ,(3L, 1L))
    val rawEdges = sc.parallelize(edges ++ edges)
    val graph = Graph.fromEdgeTuples(rawEdges, true, uniqueEdges = Some(RandomVertexCut)).cache()
    val simRankVal = simRankByGraphx(graph)

  }
}
