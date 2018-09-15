package com.demo

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 连接两个文件, 统计电影评分大于 4 的电影
  */
object FileJoin {
    def main(args: Array[String]): Unit = {
        val ratingsFile = args(0)
        val moviesFile = args(1)

        val sparkConf = new SparkConf().setAppName("FileJoin")
        val sc = new SparkContext(sparkConf)

        val ratingsData = sc.textFile(ratingsFile)
        val moviesData = sc.textFile(moviesFile)

        val ratingsValue = ratingsData.filter(_.trim().split("::").size == 3 ).map(line => {
            val fields = line.trim().split("::")
            (fields(0).toInt, fields(1))
        }).keyBy(line => line._1)

        val moviesValue = moviesData.filter(_.trim().split("::").size == 4).map(line => {
            val fields = line.trim().split("::")
            (fields(1).toInt, fields(2).toDouble)
        }).groupByKey().map(line => (line._1, (line._2.sum / line._2.size))).keyBy(line => line._1)

        val result = moviesValue.join(ratingsValue).filter(line => line._2._1._2 > 4.0).map(line => (line._1, line._2._1._2, line._2._2._2))
        result.foreach(println)
    }
}
