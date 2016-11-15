package com.dataworks.spark.rdd.pair

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
  * Created by Sandeep on 10/28/16.
  */
object reduceByKeyEx {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("reduceByKeyEx").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("OFF")

    reduceByKey(sc)
  }

  def reduceByKey(sc: SparkContext) = {
    val baseRDD: RDD[String] = sc.textFile("/Users/Sandeep/dataworks/data/spark", 3)
    val filtered: RDD[String] = baseRDD.filter(line => !line.isEmpty)
    val wordsRdd: RDD[String] = filtered.flatMap(_.split(" ")).filter(!_.isEmpty)
    val wordLenPair: RDD[(String, Int)] = wordsRdd.map(word => (word, word.length))
    val reducedRdd: RDD[(String, Int)] = wordLenPair.reduceByKey(reduceOp)
    reducedRdd.foreach(println)
  }

  def reduceOp(acc: Int, value: Int): Int = {
    acc + value
  }

  def reduceByKeyPart(sc: SparkContext) = {
    val baseRDD: RDD[String] = sc.textFile("/Users/Sandeep/dataworks/data/spark", 3)
    val filtered: RDD[String] = baseRDD.filter(line => !line.isEmpty)
    val wordsRdd: RDD[String] = filtered.flatMap(_.split(" ")).filter(!_.isEmpty)
    val wordLenPair: RDD[(String, Int)] = wordsRdd.map(word => (word, word.length))
    val reducedRdd: RDD[(String, Int)] = wordLenPair.reduceByKey(new Partitioner() {
      override def numPartitions: Int = 2

      override def getPartition(key: Any): Int = key match {
        case null => 0
        case key: String => key.charAt(0).toInt % numPartitions
        case _ => 0
      }
    }, (acc: Int, value: Int) => acc + value)
    reducedRdd.foreach(println)
  }

}
