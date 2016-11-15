package com.dataworks.spark.rdd.pair

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Sandeep on 10/28/16.
  */
object valuesEx {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("valuesEx").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("OFF")

    valuesExStr(sc)
  }

  def valuesExStr(sc: SparkContext): Unit = {
    val baseRDD: RDD[String] = sc.textFile("/Users/Sandeep/dataworks/data/spark")
    val filteredRdd: RDD[String] = baseRDD.filter(line => !line.isEmpty).flatMap(_.split(" "))
      .filter(x => !x.isEmpty)
    val pairRdd: RDD[(Int, String)] = filteredRdd.map(x => (x.length, x))
    val keys: RDD[String] = pairRdd.values.distinct()

    keys.foreach(x => println(x))

  }

}
