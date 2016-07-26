/**
  * Created by wangyan on 16-7-26.
  */

import org.apache.spark.graphx.PartitionStrategy.RandomVertexCut
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import scala.reflect.ClassTag

object LabelPropagation {

  def LPByAgg[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED],maxIter: Int): Graph[VertexId, ED] = {
    val g = graph.groupEdges((a, b) => a).cache()
    var cmty = g.mapVertices { case (vid, _) => vid }

    def send(ctx: EdgeContext[VertexId, ED, Map[VertexId, Long]]) {
      if (ctx.srcId == ctx.dstId) {
        ctx.sendToSrc(Map(ctx.srcId -> 0L))
        ctx.sendToDst(Map(ctx.dstId-> 0L))
      }
      ctx.sendToSrc(Map(ctx.dstAttr -> 1L, ctx.srcId ->0L))
      ctx.sendToDst(Map(ctx.srcAttr -> 1L, ctx.dstId ->0L))
    }

    def merge(count1: Map[VertexId, Long], count2: Map[VertexId, Long]):Map[VertexId, Long] = {
      val count = (count1.keySet ++ count2.keySet).map { i =>
        val count1Val = count1.getOrElse(i, 0L)
        val count2Val = count2.getOrElse(i, 0L)
        i -> (count1Val + count2Val)
      }.toMap
      val iter = count.iterator
      var id =1L
      while(iter.hasNext){
        val temp = iter.next()
        if (temp._2 == 0)
          id = temp._1
      }
      val maxCount = count.maxBy(_._2)._1
      Map(id->maxCount)
    }

    for (i <- 1 to maxIter) {
      val mapNum :VertexRDD[Map[VertexId, Long]] = cmty.aggregateMessages(sendMsg = send, mergeMsg = merge,tripletFields = TripletFields.All)
      val num1 :VertexRDD[Long] = mapNum.mapValues{ mapa=>
        val iter = mapa.iterator
        iter.next()._2
      }
      cmty = cmty.outerJoinVertices(num1){case (vid, oldLabel,newLabel) =>
        newLabel.getOrElse(oldLabel)
      }
    }
    return cmty
  }

  def LPByPregel[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED],maxIter : Int): Graph[VertexId, ED] = {
    val g = graph.groupEdges((a, b) => a)
    var cmty :Graph[VertexId,ED] = g.mapVertices { case (vid, _) => vid }.cache()
    val message = Map[VertexId, Long]()

    def vertexLabel(vid:VertexId,attr:Long , msg:Map[VertexId, Long]):VertexId = {
      if (msg.isEmpty) attr
      else msg.maxBy(_._2)._1
    }

    def send(etri:EdgeTriplet[VertexId, ED]): Iterator[(VertexId, Map[VertexId, Long])]= {
      if (etri.dstId == etri.srcId) Iterator()
      else Iterator((etri.srcId, Map(etri.dstAttr -> 1L)),(etri.dstId, Map(etri.srcAttr -> 1L)))
    }

    def merge(count1: Map[VertexId, Long], count2: Map[VertexId, Long]):Map[VertexId, Long] = {
      val count = (count1.keySet ++ count2.keySet).map { i =>
        val count1Val = count1.getOrElse(i, 0L)
        val count2Val = count2.getOrElse(i, 0L)
        i -> (count1Val + count2Val)
      }.toMap
      count
    }
    Pregel(cmty,message,maxIter)(vprog =vertexLabel ,sendMsg = send,mergeMsg = merge)
  }

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("LP").setMaster("local")
    val sc = new SparkContext(conf)
    val maxIter = 10
    val edges = Array((0L, 1L), (1L, 2L),(2L,0L),(2L,3L),(3L,4L),(4L,5L),(5L,3L))
    val rawEdges = sc.parallelize(edges ++ edges)
    val graph = Graph.fromEdgeTuples(rawEdges, true, uniqueEdges = Some(RandomVertexCut)).cache()
//    val community = LPByAgg(graph,maxIter)
    val community = LPByPregel(graph,maxIter).cache()
    community.vertices.collect().foreach { case(vid1,label)=>
      println(vid1+" : "+label)
    }
  }
}
