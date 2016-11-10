package com.dataworks.spark.rdd.pair

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Sandeep on 10/28/16.
  */
object groupByKeyEx {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("groupByKeyEx").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("OFF")

    groupByKey(sc)
  }

  def groupByKey(sc: SparkContext): Unit = {
    val baseRDD: RDD[String] = sc.textFile("/Users/Sandeep/dataworks/data/spark", 3)
    val filtered: RDD[String] = baseRDD.filter(line => !line.isEmpty)
    val groupedRdd: RDD[(String, Iterable[String])] = filtered.groupBy(lines => lines.split(" ")(0))

    val separator = ","
    val mapped: RDD[Array[String]] = groupedRdd.glom().map(e =>
      e.map { case (key, value) =>
        s"$key -> ${value.mkString(separator)}"
      })
    mapped.foreach(x => println(x.mkString("\n")))
  }


}
