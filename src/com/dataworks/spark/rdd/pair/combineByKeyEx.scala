package com.dataworks.spark.rdd.pair

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Sandeep on 10/28/16.
  */
object combineByKeyEx {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("fullOuterJoinEx").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("OFF")

    combineByKey(sc)
  }

  def combineByKey(sc: SparkContext): Unit = {

  }

}
