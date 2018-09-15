package com.demo

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 二次排序, 根据第一个值排序, 如果第一个值相同, 就根据第二个值排序
  */
object SecondSort {
    def main(args: Array[String]): Unit = {
        val filepath = args(0)
        val sparkConf = new SparkConf().setAppName("SecondSort")
        val sc = new SparkContext(sparkConf)

        val data = sc.textFile(filepath)
        val sortableData = data.filter(_.trim.length > 0).map(line => {
            val fields = line.split(" ")
            (new SecondOrder(fields(0).toInt, fields(1).toInt), line)
        })

        val result = sortableData.sortByKey(false).map(line => line._2)
        result.collect().foreach(println)

    }
}
