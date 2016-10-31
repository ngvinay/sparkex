package com.dataworks.spark.rdd.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Sandeep on 10/28/16.
  */
object distinctEx {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("distinctEx").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("OFF")

    distinctStr(sc)
    distinctStr2(sc)
  }

  def distinctStr(sc: SparkContext): Unit = {
    val baseRdd: RDD[String] = sc.parallelize(Array("b", "a", "c", "d", "a", "c", "f"))
    val distinctRdd: RDD[String] = baseRdd.distinct()

    distinctRdd.foreach(x => println(x))

  }

  def distinctStr2(sc: SparkContext): Unit = {
    val baseRdd: RDD[String] = sc.parallelize(Array("b", "a", "c", "d", "a", "c", "f"))
    val distinctRdd: RDD[String] = baseRdd.distinct(2)

    distinctRdd.glom.map(_.mkString(", ")).foreach(x => println(x))

  }

}
