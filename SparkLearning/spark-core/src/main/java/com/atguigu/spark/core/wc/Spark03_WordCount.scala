package com.atguigu.spark.core.wc

import org.apache.spark.{SparkConf, SparkContext}

object Spark03_WordCount {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount03")
    val sparkContext = new SparkContext(sparkConf)

    val lines = sparkContext.textFile("Datas")

    val words = lines.flatMap(_.split(" "))

    val wordToOne = words.map(word=>(word, 1))

//    val wordGroup = wordToOne.groupBy(t=>t._1)
    //reduceByKey：相同的Key的数据，可以对value进行reduce聚合
    val wordToCount = wordToOne.reduceByKey(_ + _)

    val array = wordToCount.collect()
    array.foreach(println)
    sparkContext.stop()
  }

}
