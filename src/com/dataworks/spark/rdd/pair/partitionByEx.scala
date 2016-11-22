package com.dataworks.spark.rdd.pair

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, RangePartitioner, SparkConf, SparkContext}

/**
  * Created by Sandeep on 10/28/16.
  */
object partitionByEx {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("partitionByEx").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("OFF")

    hashPartitionByEx(sc)
    rangePartitonByEx(sc)
  }


  def hashPartitionByEx(sc: SparkContext) = {
    val baseRdd: RDD[String] = sc.textFile("/Users/Sandeep/dataworks/data/this", 8)
      .filter(x => !x.isEmpty)

    val thisPair: RDD[(Int, String)] = baseRdd.flatMap(x => x.split(" ")).map(x => (x.length, x))

    val repartitioned: RDD[(Int, String)] = thisPair.partitionBy(new HashPartitioner(4))
    println(repartitioned.partitions.length)

  }

  def rangePartitonByEx(sc: SparkContext): Unit = {
    val baseRdd: RDD[String] = sc.textFile("/Users/Sandeep/dataworks/data/this", 8)
      .filter(x => !x.isEmpty)

    val thisPair: RDD[(Int, String)] = baseRdd.flatMap(x => x.split(" ")).map(x => (x.length, x))

    val repartitioned: RDD[(Int, String)] = thisPair.partitionBy(new RangePartitioner(4, thisPair))
    println(repartitioned.partitions.length)

  }

}
