package com.dataworks.spark.rdd.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Sandeep on 10/28/16.
  */
object glomEx {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("glomEx").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("OFF")

    val baseRdd: RDD[Int] = sc.parallelize(Seq(100, 1, 4, 5, 2, 99, 3, 88, 1000, 34, 24, 12, 10, 5, 0), 3)
    val maxElement: Int = baseRdd.glom().map(x => x.max).reduce((x, y) => x max y)

    println("Max in the Seq:" + maxElement)
  }

}
