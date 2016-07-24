/**
  * Created by wangyan on 16-7-23.
  */

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

object wordcount {
  def main(args: Array[String]): Unit = {

    val dirIn = "/home/wangyan/IdeaProjects/test2/in/word.txt"
    val dirOut = "/home/wangyan/IdeaProjects/test2/out/count"

    val conf = new SparkConf().setAppName("WordCount")
    val sc = new SparkContext(conf)

    val input= sc.textFile(dirIn)
    val countResult = input.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)
    countResult.collect().foreach(println)
    countResult.saveAsTextFile(dirOut)
    sc.stop()

  }
}
