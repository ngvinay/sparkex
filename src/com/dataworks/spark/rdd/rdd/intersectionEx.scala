package com.dataworks.spark.rdd.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
  * Created by Sandeep on 10/28/16.
  */
object intersectionEx {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("IntersectionEx").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("OFF")

    intersectionInt(sc)
    intersectionInt2(sc)
    intersectionInt3(sc)
  }

  def intersectionInt(sc: SparkContext): Unit = {
    val baseRdd1: RDD[Int] = sc.parallelize(1 to 10)
    val baseRdd2: RDD[Int] = sc.parallelize(5 to 15)

    val intersectRdd: RDD[Int] = baseRdd1.intersection(baseRdd2)
    val glomedRdd: RDD[String] = intersectRdd.glom().map(_.mkString(", "))
    glomedRdd.foreach(x => println(x))
  }

  def intersectionInt2(sc: SparkContext): Unit = {
    val baseRdd1: RDD[Int] = sc.parallelize(1 to 10)
    val baseRdd2: RDD[Int] = sc.parallelize(5 to 15)

    val intersectRdd: RDD[Int] = baseRdd1.intersection(baseRdd2, 2)
    val glomedRdd: RDD[String] = intersectRdd.glom().map(_.mkString(", "))
    glomedRdd.foreach(x => println(x))
  }

  def intersectionInt3(sc: SparkContext): Unit = {
    println("With partitioner")
    val baseRdd1: RDD[Int] = sc.parallelize(1 to 50, 5)
    val baseRdd2: RDD[Int] = sc.parallelize(40 to 100, 4)
    val intersectRdd = baseRdd1.intersection(baseRdd2, new Partitioner {
      override def numPartitions: Int = 2

      override def getPartition(key: Any): Int = {
        key.asInstanceOf[Int] % numPartitions
      }
    })

    val glomedRdd: RDD[String] = intersectRdd.glom().map(_.mkString(", "))
    glomedRdd.foreach(x => println(x))
  }

}
