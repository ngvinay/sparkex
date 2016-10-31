package com.dataworks.spark.rdd.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
  * Created by Sandeep on 10/28/16.
  */
object subtractEx {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("IntersectionEx").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("OFF")

    subtractInt(sc)
    subtractInt2(sc)
    subtractInt3(sc)
  }

  def subtractInt(sc: SparkContext): Unit = {
    val baseRdd1: RDD[Int] = sc.parallelize(1 to 10)
    val baseRdd2: RDD[Int] = sc.parallelize(5 to 15)

    val subtractRdd: RDD[Int] = baseRdd1.subtract(baseRdd2)
    subtractRdd.foreach(x => println(x))
  }

  def subtractInt2(sc: SparkContext): Unit = {
    val baseRdd1: RDD[Int] = sc.parallelize(1 to 10)
    val baseRdd2: RDD[Int] = sc.parallelize(5 to 15)

    val subtractRdd: RDD[Int] = baseRdd1.subtract(baseRdd2, 2)
    val glomedRdd: RDD[String] = subtractRdd.glom().map(_.mkString(", "))
    glomedRdd.foreach(x => println(x))
  }

  def subtractInt3(sc: SparkContext): Unit = {
    val baseRdd1: RDD[Int] = sc.parallelize(1 to 10)
    val baseRdd2: RDD[Int] = sc.parallelize(5 to 15)

    val subtractRdd: RDD[Int] = baseRdd1.subtract(baseRdd2, new Partitioner {
      override def numPartitions: Int = 2

      override def getPartition(key: Any): Int = {
        key.asInstanceOf[Int] % numPartitions
      }
    })
    val glomedRdd: RDD[String] = subtractRdd.glom().map(_.mkString(", "))
    glomedRdd.foreach(x => println(x))
  }

}
