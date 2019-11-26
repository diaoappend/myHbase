package com.diao.bulkload;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class BulkLoadDriver {
    private static Logger logger = LoggerFactory.getLogger(BulkLoadDriver.class);

    public static void main(String[] args) throws Exception {
        //HDFS输入输出路径由第一和第二个参数传入
        final Path inputPath= new Path(args[0]);
        final Path outputPath=  new Path(args[1]);

        Configuration conf = HBaseConfiguration.create();
        HTable table = new HTable(conf,args[2]);//表名由第三个参数传入

        //HBaseConfiguration.addHbaseResources(conf);

        Job job=Job.getInstance(conf);
        job.setJarByClass(BulkLoadDriver.class);
        job.setMapperClass(BulkLoadMapper.class);

        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);

        job.setOutputFormatClass(HFileOutputFormat2.class);


        HFileOutputFormat2.configureIncrementalLoad(job,table,table.getRegionLocator());

        FileInputFormat.addInputPath(job,inputPath);
        FileOutputFormat.setOutputPath(job,outputPath);

        if(job.waitForCompletion(true)){
            logger.info("Txt文件已转换为HFile文件，开始执行BulkLoad操作");

            LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
            loader.doBulkLoad(outputPath, table);
            table.close();
        }



    }
}