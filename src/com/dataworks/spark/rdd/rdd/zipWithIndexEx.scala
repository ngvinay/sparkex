package com.dataworks.spark.rdd.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Sandeep on 10/28/16.
  */
object zipWithIndexEx {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("zipWithIndexEx").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("OFF")

    zipIntStrPartn(sc)
  }

  def zipIntStrPartn(sc: SparkContext) = {
    val baseRDD: RDD[String] = sc.textFile("/Users/Sandeep/dataworks/data/spark", 4)
    val filteredRdd: RDD[String] = baseRDD.filter(line => !line.isEmpty)
    val flattenedRdd: RDD[String] = filteredRdd.flatMap(line => line.split(" "))


    println("Base RDD Partition cnt:" + baseRDD.partitions.length)

    val zippedRdd: RDD[(String, Long)] = flattenedRdd.zipWithIndex()
    zippedRdd.glom().map(x => x.mkString(", ")).foreach(println)

  }



}
