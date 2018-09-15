package com.demo

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 读取多个文件, 求其中的最大值, 最小值
  */
object MaxMinValue {
    def main(args: Array[String]): Unit = {

        val filepath = args(0)
        val sparkConf = new SparkConf().setAppName("MaxMinValue")
        val sc = new SparkContext(sparkConf)

        val data = sc.textFile(filepath)
        val groupResult = data.filter(line => line != None && line.length() > 0).map(line => ("key", line.toInt)).groupByKey()
        val result = groupResult.map(line => {
            val values = line._2
            var max = Int.MinValue
            var min = Int.MaxValue

            for (value <- values) {
                if (max < value) max = value
                if (min > value) min = value
            }
            (max, min)
        })

        result.foreach(line => println(line._1 + ", " + line._2))
    }


}
