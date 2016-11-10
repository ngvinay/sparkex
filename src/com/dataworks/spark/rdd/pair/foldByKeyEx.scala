package com.dataworks.spark.rdd.pair

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Sandeep on 10/28/16.
  */
object foldByKeyEx {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("OFF")

    foldByKey(sc)
  }

  def foldByKey(sc: SparkContext): Unit = {
    val baseRDD: RDD[String] = sc.textFile("/Users/Sandeep/dataworks/data/spark", 3)
    val filtered: RDD[String] = baseRDD.filter(line => !line.isEmpty)
    val wordsRdd: RDD[String] = filtered.flatMap(_.split(" ")).filter(!_.isEmpty)
    val wordPair: RDD[(Int, String)] = wordsRdd.map(word => (word.length, word))

    val foldedRdd: RDD[(Int, String)] = wordPair.foldByKey(" ")(seqOp)
    foldedRdd.foreach(println)

  }

  def seqOp(acc: String, valu: String): String = {
    if (acc.equals(" "))
      acc + valu.trim
    else
      acc + "*" + valu.trim
  }

}
