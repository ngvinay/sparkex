package com.dataworks.spark.rdd.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Sandeep on 10/28/16.
  */
object flatMapEx {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("flatMapEx").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("OFF")

    faltMapInt(sc)
    flatMapString(sc)
  }

  def faltMapInt(sc: SparkContext) = {
    val baseRdd: RDD[Int] = sc.parallelize(1 to 3)
    val flattendRdd: RDD[Int] = baseRdd.flatMap(x => Array(x, x * 10, x * 10 + 4))

    flattendRdd.foreach(x => println(x))


  }

  def flatMapString(sc: SparkContext): Unit = {
    val baseRdd: RDD[String] = sc.parallelize(Array("When I'm old and mankey",
      "I'll never use a hanky.",
      "I'll wee on plants",
      "and soil my pants",
      "and sometimes get quite cranky"))
    val flattendRdd: RDD[String] = baseRdd.flatMap(x => x.split(" "))
    flattendRdd.foreach(x => println(x))

  }

}
