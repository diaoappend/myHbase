package com.diao.bulkload.spark

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkConf, SparkContext}


object HFile2HBase {

  val conf = HBaseConfiguration.create()
  val tableName = "ztbsj"
  val htable = new HTable(conf,tableName)
  conf.set(TableOutputFormat.OUTPUT_TABLE,tableName)
  val job = Job.getInstance(conf)
  job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
  job.setMapOutputValueClass(classOf[Put])
  HFileOutputFormat.configureIncrementalLoad (job, htable)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("bulkload").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("hdfs://10.170.128.57:8020/tmp/input1")
    val rdd = lines.map(x=>{
      val fields = x.toString.split(";")
      if(fields.length>=6){
        val rowkey:String = (new StringBuffer().append(fields(1)).reverse().toString)+"|"+fields(4)+"|"+fields(0)
        print(rowkey)
        val put = new Put(Bytes.toBytes(rowkey))
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("ID"),fields(0).getBytes())
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("GCXM"),fields(1).getBytes())
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("XMMC"),fields(2).getBytes())
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("XMJC"),fields(3).getBytes())
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("BDBH"),fields(4).getBytes())
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("QDBH"),fields(5).getBytes())
        (new ImmutableBytesWritable(Bytes.toBytes(rowkey)),put)
      }
    })
  }


  //rdd.saveAsNewAPIHadoopFile("/tmp/output", classOf[ImmutableBytesWritable], classOf[Put], classOf[HFileOutputFormat], conf)
}
