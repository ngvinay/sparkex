package com.dataworks.spark.rdd.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Sandeep on 10/28/16.
  */
object zipEx {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("zipEx").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("OFF")

    zipIntStr(sc)
  }

  def zipIntStr(sc: SparkContext) = {
    val baseRDD: RDD[String] = sc.textFile("/Users/Sandeep/dataworks/data/spark", 4)
    val filteredRdd: RDD[String] = baseRDD.filter(line => !line.isEmpty)
    val flattenedRdd: RDD[String] = filteredRdd.flatMap(line => line.split(" "))
    val sizeRdd: RDD[Int] = flattenedRdd.map(x => x.length)

    println("Base RDD Partition cnt:" + baseRDD.partitions.length + " size RDD Partition " +
      "Cnt:" + sizeRdd.partitions.length)

    val zippedRdd: RDD[(String, Int)] = flattenedRdd.zip(sizeRdd)
    zippedRdd.glom().map(x => x.mkString(", ")).foreach(println)

  }

}
