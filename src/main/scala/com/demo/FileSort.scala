package com.demo

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
  * 将文件中的整数排序, 并指定序号
  */
object FileSort {
    def main(args: Array[String]): Unit = {
        val filepath = args(0)
        val sparkConf = new SparkConf().setAppName("FileSort")
        val sc = new SparkContext(sparkConf)

        val data = sc.textFile(filepath, 3)
        val sortData = data.filter(_.trim.length > 0).map(line => (line.toInt, "")).partitionBy(new HashPartitioner(1)).sortByKey()

        var index = 0
        val result = sortData.map(line => {
            index += 1
            (index, line._1)
        }).foreach(x => println(x._1 + ", " + x._2))
    }
}

