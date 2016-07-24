/**
  * Created by wangyan on 16-7-23.
  */

import org.apache.spark._
import scala.collection.mutable.ArrayBuffer
import SparkContext._
import org.apache.spark.rdd.RDD

object SimRank {

  //change simrank values of (i,i) to 1.0
  def change(s:(String, Double)): (String, Double) ={
    val line = s._1.split('_')
    if (line(0)==line(1)) (s._1, 1.0)
    else s
  }


  def main(args: Array[String]) {

    val C = 0.8
    val iters = 10
    val conf = new SparkConf().setAppName("SimRankTest")
    val sc = new SparkContext(conf)

    val graphLine = sc.textFile("/home/wangyan/IdeaProjects/test2/in/graphLine.txt")
    val outEdges = graphLine.map{s=> (s.split(':')(0),s.split(':')(1))}.cache()
    //node pairs and their outdegree node pairs, which combine with each outdegree nodes of these two nodes
    val graph = outEdges.cartesian(outEdges).flatMap{s=>
      val line1 = s._1._2.split(',')
      val line2 = s._2._2.split(',')
      val list = ArrayBuffer[String]()
      for(x <- line1){
        for(y <- line2){
          list += x + '_' + y
        }
      }
      list.map{f=> (s._1._1 + '_' + s._2._1, f)}
    }.groupByKey().cache()

    val inDegree = outEdges.map{s=>(s._1, s._2.split(',').length)}
    //indegree product between two nodes
    val inDegreePair = inDegree.cartesian(inDegree).map{s=>(s._1._1+'_'+s._2._1, s._1._2*s._2._2)}.cache()

    val nodeList = outEdges.keys
    // initialize simrank values
    var simrank = nodeList.cartesian(nodeList).map{s=>
      if(s._1==s._2) (s._1 + '_' +s._2, 1.0)
      else (s._1 + '_' +s._2, 0.0)
    }.cache()

    //iteratively update simrank values
    for (i<- 1 to iters){
      val con = graph.join(simrank).values.flatMap{case (urls, sim)=>  urls.map(url=>(url,sim))}
      //update simrank values with definition
      simrank = con.reduceByKey(_+_).join(inDegreePair).mapValues{s=>
        if (s._2 >10e-10) s._1/s._2 * C
        else 0.0
      }
      simrank=simrank.map(s => change(s))
      simrank.foreach(println _)
    }
    //simrank.saveAsTextFile("/home/wangyan/IdeaProjects/test2/out/simrank")
    sc.stop()
  }
}
