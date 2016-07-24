/**
  * Created by wangyan on 16-7-23.
  */

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

object learn {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("learn").setMaster("local")
    val sc = new SparkContext(conf)

    val data = Array( 1, 2, 3, 4, 5)
    val distData = sc. parallelize( data)
    distData.reduce( ( a, b) => a + b)
    distData.collect().foreach(println)
  }
}
