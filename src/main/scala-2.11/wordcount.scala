import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by wangyan on 16-7-23.
  */
object wordcount {
  def main(args: Array[String]): Unit = {

    val dirIn = "hdfs://localhost:8080/home/wangyan/IdeaProjects/test2/in/word.txt"
    val dirOut = "hdfs://localhost:8080/home/wangyan/IdeaProjects/test2/out/count"

    val conf = new SparkConf().setAppName("WordCount")
    val sc = new SparkContext(conf)

    val line = sc.textFile(dirIn)
    val cnt = line.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _) // 文件按空格拆分，统计单词次数
    val sortedCnt = cnt.map(x => (x._2, x._1)).sortByKey(ascending = false).map(x => (x._2, x._1)) // 按出现次数由高到低排序

    sortedCnt.collect().foreach(println) // 控制台输出
    sortedCnt.saveAsTextFile(dirOut) // 写入文本文件
    sc.stop()

  }
}
