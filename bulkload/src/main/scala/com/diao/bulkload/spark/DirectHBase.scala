package com.diao.bulkload.spark

import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue}
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat
import org.apache.hadoop.mapreduce.Job



class DirectHBase {

  val conf = HBaseConfiguration.create()
  val tableName = "ztbsj"
  val htable = new HTable(conf,tableName)
  conf.set(TableOutputFormat.OUTPUT_TABLE,tableName)
  val job = Job.getInstance(conf)
  job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
  job.setMapOutputValueClass(classOf[KeyValue])
  HFileOutputFormat.configureIncrementalLoad (job, htable)


}
