package com.atguigu.spark.core.wc

import org.apache.spark.{SparkConf, SparkContext}

object Spark02_WordCount {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount02")
    val sparkContext = new SparkContext(sparkConf)

    val lines = sparkContext.textFile("Datas")

    val words = lines.flatMap(_.split(" "))

    val wordToOne = words.map(word=>(word, 1))

    val wordGroup = wordToOne.groupBy(t=>t._1)

    val wordToCount = wordGroup.map {
      case (word, list) => {
        list.reduce(
          (t1, t2) => (t1._1, t1._2 + t2._2)
        )
      }
    }
    val array = wordToCount.collect()
    array.foreach(println)
    sparkContext.stop()
  }

}
