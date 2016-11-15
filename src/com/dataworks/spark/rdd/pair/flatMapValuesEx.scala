package com.dataworks.spark.rdd.pair

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Sandeep on 10/28/16.
  */
object flatMapValuesEx {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("mapValuesEx").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("OFF")

    flatMapValStr(sc)
  }

  def flatMapValStr(sc: SparkContext) = {
    val baseRDD: RDD[String] = sc.textFile("/Users/Sandeep/dataworks/data/spark")
    val filteredRdd: RDD[String] = baseRDD.filter(line => !line.isEmpty).flatMap(_.split(" "))
    val pairRdd: RDD[(Int, String)] = filteredRdd.map(x => (x.length, x))
    val valuesRdd: RDD[(Int, String)] = pairRdd.flatMapValues(x => Seq(x.toUpperCase, x
      .toLowerCase))

    valuesRdd.foreach(x => println(x))

  }

  def flatMapValInt(sc: SparkContext) = {
    val baseRdd: RDD[Int] = sc.parallelize(1 to 5)
    val mappedRdd: RDD[(Int, Int)] = baseRdd.map(x => (x, x + 1))

    val flattenedValRdd: RDD[(Int, Int)] = mappedRdd.flatMapValues(x => x to 10)

    flattenedValRdd.foreach(x => print(x + "\n"))

  }

}
