package com.dataworks.spark.rdd.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Sandeep on 10/28/16.
  */
object coalesceEx {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("coalesceEx").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("OFF")

    coalesce(sc)
  }

  def coalesce(sc: SparkContext): Unit = {
    val baseRdd: RDD[Int] = sc.parallelize(1 to 100, 5)
    println("Base RDD Partition size :" + baseRdd.partitions.length)

    val repartRdd: RDD[Int] = baseRdd.coalesce(2, shuffle = true)
    println("Coalesced RDD Partition size :" + repartRdd.partitions.length)
    println(repartRdd.toDebugString)
  }

}
