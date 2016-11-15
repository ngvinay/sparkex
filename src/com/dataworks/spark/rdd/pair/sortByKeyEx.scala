package com.dataworks.spark.rdd.pair

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Sandeep on 11/15/16.
  */
object sortByKeyEx {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("sortByKeyEx").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("OFF")

    sortKeyStr(sc)
  }

  def sortKeyStr(sc: SparkContext): Unit = {
    val baseRDD: RDD[String] = sc.textFile("/Users/Sandeep/dataworks/data/spark")
    val filteredRdd: RDD[String] = baseRDD.filter(line => !line.isEmpty).flatMap(_.split(" "))
      .filter(x => !x.isEmpty)
    val pairRdd: RDD[(Int, String)] = filteredRdd.map(x => (x.length, x))
    val sortedRdd: RDD[(Int, String)] = pairRdd.sortByKey(ascending = true, numPartitions = 3).distinct()

    sortedRdd.foreach(x => println(x))
  }

}
