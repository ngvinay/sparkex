package com.dataworks.spark.rdd.pair

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Sandeep on 10/28/16.
  */
object lookupEx {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("lookupEx").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("OFF")

    lookupKey(sc)
  }

  def lookupKey(sc: SparkContext): Unit = {
    val baseRDD: RDD[String] = sc.textFile("/Users/Sandeep/dataworks/data/spark")
    val filteredRdd: RDD[String] = baseRDD.filter(line => !line.isEmpty).flatMap(_.split(" "))
      .filter(x => !x.isEmpty)
    val pairRdd: RDD[(String, Int)] = filteredRdd.map(x => (x, x.length))

    val lookedUp: Seq[Int] = pairRdd.lookup("this")
    lookedUp.foreach(x => println(x))
  }

}
