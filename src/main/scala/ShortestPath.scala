/**
  * Created by wangyan on 16-7-27.
  */

import org.apache.spark.graphx.PartitionStrategy.RandomVertexCut
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators

import scala.reflect.ClassTag

object ShortestPath {

  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, Double],sourceId : Long): Graph[Double, Double] = {
    val g = graph.groupEdges((a, b) => a)
    val initialGraph = graph.mapVertices((vid , dis)=> if (vid == sourceId) 0.0 else Double.PositiveInfinity).cache()
    val message = Double.PositiveInfinity

    def vertexLabel(vid:VertexId,attr:Double, msg:Double):Double = {
      Math.min(attr,msg)
    }

    def send(e:EdgeTriplet[Double, Double]): Iterator[(VertexId, Double)]= {
      if (e.dstAttr > e.srcAttr + e.attr) Iterator((e.dstId ,(e.srcAttr + e.attr)))
      else Iterator()
    }

    def merge(dis1: Double, dis2: Double):Double = {
      Math.min(dis1,dis2)
    }

    Pregel(initialGraph, message)(vprog =vertexLabel ,sendMsg = send,mergeMsg = merge)
  }

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("LP").setMaster("local")
    val sc = new SparkContext(conf)
    val vertexCount = 100
    val sourceId = 1L
    val graph = GraphGenerators.logNormalGraph(sc,vertexCount).mapEdges(e=>e.attr.toDouble)

    val sp = run(graph,sourceId).cache()
    sp.vertices.collect().foreach { case(vid,dis)=>
      println(vid+" : "+dis)
    }
  }

}
