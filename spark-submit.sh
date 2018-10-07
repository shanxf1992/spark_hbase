#!/bin/bash
/export/software/spark-2.1.0/bin/spark-submit \
	--driver-class-path /export/software/spark-2.1.0/jars/hbase*:/export/software/hbase-1.1.5/conf \
	--class com.hbase.SparkWriteToHbase \
	/export/software/SparkHBase.jar
