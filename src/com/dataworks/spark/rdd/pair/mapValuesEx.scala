package com.dataworks.spark.rdd.pair

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Sandeep on 10/28/16.
  */
object mapValuesEx {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("mapValuesEx").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("OFF")

    mapValStr(sc)
  }

  def mapValStr(sc: SparkContext): Unit = {
    val baseRDD: RDD[String] = sc.textFile("/Users/Sandeep/dataworks/data/spark")
    val filteredRdd: RDD[String] = baseRDD.filter(line => !line.isEmpty).flatMap(_.split(" "))
    val pair: RDD[(Int, String)] = filteredRdd.map(x => (x.length, x))

    val mappedValRdd: RDD[(Int, String)] = pair.mapValues(x => x.toUpperCase)
    val mappedVal: RDD[Array[String]] = mappedValRdd.glom().map(e => {
      e.map { case (key, value) => s"$key -> $value" }
    })

    mappedVal.foreach(e => println(e.mkString("\n")))

  }

}
