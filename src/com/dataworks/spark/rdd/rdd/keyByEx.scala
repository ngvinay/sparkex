package com.dataworks.spark.rdd.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Sandeep on 10/28/16.
  */
object keyByEx {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("groupByEx").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("OFF")

    keyByStr(sc)
  }

  def keyByStr(sc: SparkContext): Unit = {
    val baseRDD: RDD[String] = sc.textFile("/Users/Sandeep/dataworks/data/spark", 4)
    val filteredRdd: RDD[String] = baseRDD.filter(line => !line.isEmpty).flatMap(lines => lines.split(" "))
    val pair: RDD[(Char, String)] = filteredRdd.keyBy((word: String) => word(0))


    pair.foreach { case (x, y) => println(s"$x and $y") }

  }

  def keyByInt(sc: SparkContext): Unit = {
    val baseRdd: RDD[Int] = sc.parallelize(1 to 25)
    val pair: RDD[(Int, Int)] = baseRdd.keyBy(x => x)

    pair.foreach { case (x, y) => println(s"$x and $y") }

  }

}
