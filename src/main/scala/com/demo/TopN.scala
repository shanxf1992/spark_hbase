package com.demo

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 求多个文件中第三列的前N个最大值
  */
object TopN {
    def main(args: Array[String]): Unit = {

        val filepath = args(0)
        val sparkConf = new SparkConf().setAppName("TopN")
        val sc = new SparkContext(sparkConf)

        val data = sc.textFile(filepath)
        val sortData = data.filter(line => line.trim().length > 0 && line.trim().split(",").size == 4).map(line => {
            val value = line.trim().split(",")(2).toInt
            (value, "")
        }).sortByKey(false)

        sortData.map(value => value._1).take(10).foreach(println(_))

    }

}
