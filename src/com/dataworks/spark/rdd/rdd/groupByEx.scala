package com.dataworks.spark.rdd.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
  * Created by Sandeep on 10/28/16.
  */
object groupByEx {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("groupByEx").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("OFF")

    groupByStrPartition(sc)

  }

  def groupByStrPartition(sc: SparkContext): Unit = {
    val baseRDD: RDD[String] = sc.textFile("/Users/Sandeep/dataworks/data/spark", 4)
    val filteredRdd: RDD[String] = baseRDD.filter(line => !line.isEmpty)
    val flattenedRdd: RDD[String] = filteredRdd.flatMap(line => line.split(" "))

    val groupedRdd: RDD[(Char, Iterable[String])] = flattenedRdd.groupBy((word: String) => word(0), new Partitioner {
      override def numPartitions: Int = 2

      override def getPartition(key: Any): Int = key match {
        case null => 0
        case key: Char => key.asInstanceOf[Char].toInt % numPartitions
        case _ => key.asInstanceOf[Char].toInt % numPartitions
      }
    })

    groupedRdd.glom().map(x => x.mkString(" ,")).foreach(x => println(x))

  }

  def groupByStr(sc: SparkContext): Unit = {
    val baseRDD: RDD[String] = sc.textFile("/Users/Sandeep/dataworks/data/spark", 4)
    val filteredRdd: RDD[String] = baseRDD.filter(line => !line.isEmpty)
    val groupedRdd: RDD[(Char, Iterable[String])] = filteredRdd.flatMap(line => line.split(" ")).groupBy(word => word(0))

    groupedRdd.glom().map(x => x.mkString(" ,")).foreach(x => println(x))
  }
}
