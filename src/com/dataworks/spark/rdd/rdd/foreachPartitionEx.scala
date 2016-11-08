package com.dataworks.spark.rdd.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Sandeep on 10/28/16.
  */
object foreachPartitionEx {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("foreachPartitionEx").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("OFF")

    val baseRdd: RDD[Int] = sc.parallelize(1 to 24, 4)
    baseRdd.foreachPartition { partition => partition.foreach(x => println(x)) }

  }

}
