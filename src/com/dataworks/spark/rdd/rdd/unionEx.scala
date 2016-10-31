package com.dataworks.spark.rdd.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Sandeep on 10/28/16.
  */
object unionEx {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("UnionEx").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("OFF")

    unionDuplicates(sc)
    unionNoDuplicates(sc)
    unionOperator(sc)

  }

  def unionDuplicates(sc: SparkContext): Unit = {
    val baseRdd1: RDD[Int] = sc.parallelize(1 to 4, 2)
    val baseRdd2: RDD[Int] = sc.parallelize(3 to 8, 1)
    val unionRdd: RDD[Int] = baseRdd1.union(baseRdd2)

    val glomedRdd: RDD[String] = unionRdd.glom().map(_.mkString(", "))
    glomedRdd.foreach(x => println(x))
  }

  def unionNoDuplicates(sc: SparkContext): Unit = {
    println("Removing duplicates")
    val baseRdd1: RDD[Int] = sc.parallelize(1 to 4, 2)
    val baseRdd2: RDD[Int] = sc.parallelize(3 to 8, 1)
    val unionRdd: RDD[Int] = baseRdd1.union(baseRdd2).distinct(2)


    val glomedRdd2: RDD[String] = unionRdd.glom().map(_.mkString(", "))
    glomedRdd2.foreach(x => println(x))

  }

  def unionOperator(sc: SparkContext): Unit = {
    val baseRdd1: RDD[Int] = sc.parallelize(1 to 4, 2)
    val baseRdd2: RDD[Int] = sc.parallelize(3 to 8, 1)
    val unionRdd: RDD[Int] = baseRdd1 ++ baseRdd2

    val glomedRdd: RDD[String] = unionRdd.glom().map(_.mkString(", "))
    glomedRdd.foreach(x => println(x))

  }

}
