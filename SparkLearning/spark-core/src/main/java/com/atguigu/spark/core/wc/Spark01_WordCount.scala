package com.atguigu.spark.core.wc

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_WordCount {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount01")
    val sparkContext = new SparkContext(sparkConf)

    val lines = sparkContext.textFile("Datas")

    val words = lines.flatMap(_.split(" "))

    val wordGroup = words.groupBy(word=>word)

    val wordToWordGroup = wordGroup.map {
      case (word, list) => (word, list.size)
    }

    val array = wordToWordGroup.collect()

    array.foreach(println)
    sparkContext.stop()
  }

}
