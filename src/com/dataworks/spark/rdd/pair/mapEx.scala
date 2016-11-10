package com.dataworks.spark.rdd.pair

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Sandeep on 11/10/16.
  */
object mapEx {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("mapEx").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("OFF")

    createPair(sc)
  }

  def createPair(sc: SparkContext): Unit = {
    val baseRDD: RDD[String] = sc.textFile("/Users/Sandeep/dataworks/data/spark")
    val filteredRdd: RDD[String] = baseRDD.filter(line => !line.isEmpty)
    val word2LineRdd: RDD[(String, String)] = filteredRdd.map(lines => (lines.split(" ")(0), lines))
    val char2WordRdd: RDD[(Char, String)] = word2LineRdd.map { x =>
      (x._1.charAt(0), x._2.split(" " + " ")(0))
    }
    char2WordRdd.foreach(println)
    println(char2WordRdd.toDebugString)
  }

}
