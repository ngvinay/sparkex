package com.dataworks.spark.rdd.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Sandeep on 10/28/16.
  */
object zipPartitionsEx {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("zipPartitionsEx").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("OFF")

    zipIntStrPartn(sc)
  }

  def zipIntStrPartn(sc: SparkContext) = {
    val baseRDD: RDD[String] = sc.textFile("/Users/Sandeep/dataworks/data/spark", 4)
    val filteredRdd: RDD[String] = baseRDD.filter(line => !line.isEmpty)
    val flattenedRdd: RDD[String] = filteredRdd.flatMap(line => line.split(" "))
    val sizeRdd: RDD[Int] = flattenedRdd.map(x => x.length)

    println("Base RDD Partition cnt:" + baseRDD.partitions.length + " size RDD Partition " +
      "Cnt:" + sizeRdd.partitions.length)

    val zippedRdd: RDD[(String, Int)] = flattenedRdd.zipPartitions(sizeRdd)(zipit)
    zippedRdd.glom().map(x => x.mkString(", ")).foreach(println)

  }

  def zipit(thisIt: Iterator[String], thatIt: Iterator[Int]): Iterator[(String, Int)] = {
    var pair: List[(String, Int)] = List[(String, Int)]()
    while (thisIt.hasNext && thatIt.hasNext) {
      pair = pair :+ (thisIt.next(), thatIt.next())
    }
    pair.iterator
  }

}
