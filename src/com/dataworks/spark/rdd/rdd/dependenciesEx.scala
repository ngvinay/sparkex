package com.dataworks.spark.rdd.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{Dependency, SparkConf, SparkContext}

/**
  * Created by Sandeep on 10/28/16.
  */
object dependenciesEx {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("dependenciesEx").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("OFF")

    val baseRdd: RDD[Int] = sc.parallelize(1 to 100, 10)
    val mappedRdd: RDD[Int] = baseRdd.map(x => x + 2)
    val repartRdd: RDD[Int] = mappedRdd.repartition(5)
    val dependencies: Seq[Dependency[_]] = repartRdd.dependencies


    repartRdd.dependencies.map(_.rdd).foreach(println)
    println(repartRdd.toDebugString)
  }

}
