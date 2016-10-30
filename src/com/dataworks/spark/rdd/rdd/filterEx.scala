package com.dataworks.spark.rdd.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Sandeep on 10/28/16.
  */
object filterEx {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("filterEx").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("OFF")

    filterInt(sc)
    filterString(sc)

  }

  /**
    * Filters the input RDD using the function to evaluate whether a given element is even or not
    *
    * @param sc
    */
  def filterInt(sc: SparkContext): Unit = {
    val baseRdd: RDD[Int] = sc.parallelize(1 to 20)
    val filteredRdd: RDD[Int] = baseRdd.filter(x => x % 2 == 0)

    filteredRdd.foreach(x => println(x))

  }

  /**
    * Filters the input RDD using the function to evaluate whether
    * a given element contains either swim or man at given index
    *
    * @param sc
    */
  def filterString(sc: SparkContext): Unit = {
    val baseRdd: RDD[String] = sc.parallelize(Array("fish swim", "cats meow", "dogs bark", "man", "human"))
    val filteredRdd: RDD[String] = baseRdd.filter(x => {
      val splits: Array[String] = x.split(" ")
      if (splits.length > 1)
        splits(1).equals("swim")
      else
        splits(0).contains("man")
    })
    filteredRdd.foreach(x => println(x))
  }

}
