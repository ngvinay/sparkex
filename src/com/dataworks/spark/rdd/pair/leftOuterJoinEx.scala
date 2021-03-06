package com.dataworks.spark.rdd.pair

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Sandeep on 10/28/16.
  */
object leftOuterJoinEx {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("leftOuterJoinEx").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("OFF")

    leftJoinEx(sc)
  }

  def leftJoinEx(sc: SparkContext): Unit = {
    val thisRDD: RDD[String] = sc.textFile("/Users/Sandeep/dataworks/data/this")
      .filter(x => !x.isEmpty)
    val thatRDD: RDD[String] = sc.textFile("/Users/Sandeep/dataworks/data/that")
      .filter(x => !x.isEmpty)

    val thisPair: RDD[(Int, String)] = thisRDD.flatMap(x => x.split(" ")).map(x => (x.length, x))
    val thatPair: RDD[(Int, String)] = thatRDD.flatMap(x => x.split(" ")).map(x => (x.length, x))

    val leftJoinedRdd: RDD[(Int, (String, Option[String]))] = thisPair.leftOuterJoin(thatPair)
    leftJoinedRdd.foreach(x => println(x))
  }

}
