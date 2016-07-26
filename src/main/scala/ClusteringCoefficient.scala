/**
  * Created by wangyan on 16-7-23.
  */

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.graphx.{VertexRDD, _}
import org.apache.spark.graphx.PartitionStrategy.RandomVertexCut
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

object ClusteringCoefficient {

  def myCC [VD: ClassTag, ED: ClassTag](graph: Graph[VD,ED]): Graph[Double,ED] = {
    val g = graph.groupEdges((a, b) => (a)).cache()

    // Construct map representations of the neighborhoods
    // key in the map: vertex ID of a neighbor
    // value in the map: number of edges between the vertex and the corresonding nighbor
    val nbrMaps: VertexRDD[Map[VertexId, Int]] =
    g.collectNeighborIds(EdgeDirection.Either).mapValues { (vid, nodes) =>
      var nMap = Map.empty[VertexId, Int]
      for (i <- 0 to nodes.size-1) {
        if (nodes(i) != vid)
          nMap += (nodes(i) -> (nMap.getOrElse(nodes(i), 0) + 1))
      }
      nMap
    }

    //graph with neighbor map
    val graphNbr: Graph[Map[VertexId, Int], ED] =
    g.outerJoinVertices(nbrMaps) { (vid, _, nbr) =>
      nbr.getOrElse(null)
    }

    //count triangle number for each vertices
    def countFunc(ctx: EdgeContext[Map[VertexId, Int], ED, Double]) = {
      if(ctx.srcId == ctx.dstId) {
        ctx.sendToSrc(0)
        ctx.sendToDst(0)
      }
      val (smallId, largeId , smallMap, largeMap) =
        if(ctx.srcId > ctx.dstId) (ctx.srcId, ctx.dstId, ctx.srcAttr, ctx.dstAttr)
        else (ctx.dstId, ctx.srcId, ctx.dstAttr, ctx.srcAttr)
      var smallCount ,largeCount = 0
      var iter = smallMap.iterator
      while(iter.hasNext){
        val element = iter.next()
        val vid = element._1
        val smallVal = element._2
        val largeVal = largeMap.getOrElse(vid,0)
        if(largeVal>0 && vid != ctx.srcId && vid != ctx.dstId){
          smallCount += smallVal
          largeCount += largeVal
        }

      }
      if(smallId == ctx.srcId){
        ctx.sendToSrc(smallCount)
        ctx.sendToDst(largeCount)
      }else{
        ctx.sendToSrc(largeCount)
        ctx.sendToDst(smallCount)
      }
    }

    //number of neighbors
    var nbrNum = Map[VertexId, Int]()
    nbrMaps.collect().foreach { case (vid, nbVal) =>
      nbrNum += (vid -> nbVal.size)
    }

    val counts: VertexRDD[Double] = graphNbr.aggregateMessages(countFunc, _ + _)

    //combine cc with triangle and neighbor
    val cc = g.outerJoinVertices(counts) { case (vid, _, optCounter: Option[Double]) =>
      val dblCount: Double = optCounter.getOrElse(0)
      val nbNum = nbrNum(vid)
      if (nbNum > 1) {
        dblCount / (2 * nbNum * (nbNum - 1))
      }
      else {
        0
      }
    }
    return  cc
  }



  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("learn").setMaster("local")
    val sc = new SparkContext(conf)

    // build a triangle graph with duplicated edges
    val edges = Array((0L, 1L), (1L, 2L), (2L, 0L))
    val rawEdges = sc.parallelize(edges ++ edges)
    val graph =Graph.fromEdgeTuples(rawEdges,true,uniqueEdges =Some(RandomVertexCut)).cache()
    //output results
    val ccs = myCC(graph)
    ccs.vertices.foreach{case (vid, cc)=>
      println(vid +" : " + cc)
    }

  }
}
