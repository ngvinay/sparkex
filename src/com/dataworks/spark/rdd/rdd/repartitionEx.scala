package com.dataworks.spark.rdd.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Sandeep on 10/28/16.
  */
object repartitionEx {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("repartitionEx").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("OFF")

    repart(sc)
  }

  def repart(sc: SparkContext): Unit = {
    val baseRdd: RDD[Int] = sc.parallelize(1 to 100, 5)
    println("Base RDD Partition size :" + baseRdd.partitions.length)

    val repartRdd: RDD[Int] = baseRdd.repartition(2)
    println("Repartition RDD Partition size :" + repartRdd.partitions.length)
    println(repartRdd.toDebugString)
  }

}
