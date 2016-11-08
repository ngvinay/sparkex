package com.dataworks.spark.rdd.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Sandeep on 10/28/16.
  */
object persistEx {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("persistEx").setMaster("local")
    conf.set("spark.local.dir", "/Users/Sandeep/dataworks/ops/spark")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("OFF")

    persistMem(sc)
    persistDisk(sc)
    persistMemAndDisk(sc)

  }

  def persistMem(sc: SparkContext): Unit = {
    val baseRdd: RDD[Int] = sc.parallelize(1 to 25)
    baseRdd.persist(StorageLevel.MEMORY_ONLY)
  }

  def persistDisk(sc: SparkContext): Unit = {
    val baseRdd: RDD[Int] = sc.parallelize(1 to 25)
    val persistedRdd: RDD[Int] = baseRdd.persist(StorageLevel.DISK_ONLY)

    println("Persist element count:" + persistedRdd.count())
  }

  def persistMemAndDisk(sc: SparkContext): Unit = {
    val baseRdd: RDD[Int] = sc.parallelize(1 to 25)
    baseRdd.persist(StorageLevel.MEMORY_AND_DISK)
  }

}
