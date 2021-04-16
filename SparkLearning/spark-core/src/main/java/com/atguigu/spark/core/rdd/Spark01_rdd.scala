package com.atguigu.spark.core.rdd

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_rdd {

  def main(args: Array[String]): Unit = {
    //环境准备
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val list = List(1,2,3,4)
    //创建RDD
    //1.在内存中创建RDD
    //1.1 parallelize方式
    val rdd1 = sc.parallelize(list)
    rdd1.collect().foreach(println)

    //1.2 makeRDD方式，本质上还是调用parallelize
    val rdd2 = sc.makeRDD(list)
    rdd2.collect().foreach(println)

    //2 从外部存储创建RDD

    sc.stop()

  }

}
