package com.hbase

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.spark._
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes

/**
  * spark 程序向 HBase 中写入数据
  */
object SparkWriteToHbase {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("SparkWriteToHbase")
    val sc = new SparkContext(sparkConf)

    // 定义写入到 hbase 中表的名称
    val tablename = "student"
    sc.hadoopConfiguration.set(TableOutputFormat.OUTPUT_TABLE, tablename)

    //设置作业的配置信息
    val job = new Job(sc.hadoopConfiguration)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable]) // 指定输出的类
    job.setOutputValueClass(classOf[Result]) //指定值的类
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]]) // 指定输出的格式的类

    // 构建一个两行的记录 ( makeRDD 类似 parallelize, 可以指定分区信息)
    val indataRDD = sc.makeRDD(Array("3,LiuBei,M,24", "4,Guanyu,M,26"))
    val rdd = indataRDD.map(_.split(",")).map(arr => {
      // 设置行建的值,
      val put = new Put(Bytes.toBytes(arr(0)))
      // 分别设置对应列族下的列的值
      put.add(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(arr(1)))
      put.add(Bytes.toBytes("info"), Bytes.toBytes("gender"), Bytes.toBytes(arr(2)))
      put.add(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes(arr(3)))
      //
      (new ImmutableBytesWritable(), put)
    })

    rdd.saveAsNewAPIHadoopDataset(job.getConfiguration)
  }
}
