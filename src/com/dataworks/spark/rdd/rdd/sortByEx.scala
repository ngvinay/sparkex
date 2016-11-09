package com.dataworks.spark.rdd.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Sandeep on 10/28/16.
  */
object sortByEx {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("sortByEx").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("OFF")

    sortByStr(sc)
  }

  def sortByStr(sc: SparkContext): Unit = {
    val baseRDD: RDD[String] = sc.textFile("/Users/Sandeep/dataworks/data/spark", 4)
    val filteredRdd: RDD[String] = baseRDD.filter(line => !line.isEmpty)
    val flattenedRdd: RDD[String] = filteredRdd.flatMap(line => line.split(" "))
    val descendingRdd: RDD[String] = flattenedRdd.sortBy(word => word(0), ascending = false, 3)
    descendingRdd.glom().map(x => x.mkString(", ")).foreach(println)
  }

}
