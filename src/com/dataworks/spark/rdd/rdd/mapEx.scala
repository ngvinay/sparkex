package com.dataworks.spark.rdd.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Sandeep on 10/28/16.
  */
object mapEx {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("mapEx").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("OFF")

    map2reduce(sc)
    map2Pair(sc)
  }

  def map2Pair(sc: SparkContext): Unit = {
    val baseRdd: RDD[String] = sc.parallelize(Array("b", "a", "c"))
    val mappedRdd: RDD[(String, Int)] = baseRdd.map(x => (x, 1))
    mappedRdd.foreach(x => println(x))

  }

  def map2reduce(sc: SparkContext) = {
    val baseRdd: RDD[String] = sc.parallelize(Array("fish swim", "cats meow", "dogs bark", "man"))
    val mappedRdd: RDD[String] = baseRdd.map(x => {
      val splits: Array[String] = x.split(" ")
      if (splits.length > 1)
        splits(1)
      else
        splits(0)
    })
    mappedRdd.foreach(x => println(x))
  }

}
