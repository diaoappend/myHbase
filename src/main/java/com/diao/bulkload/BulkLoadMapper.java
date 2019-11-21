package com.diao.bulkload;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class BulkLoadMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
    private static final byte[] COLUNMFAMILY = Bytes.toBytes("info"); //列族
    private static final byte[] GCXM = Bytes.toBytes("GCXM"); //字段
    private static final byte[] XMMC = Bytes.toBytes("XMMC");
    private static final byte[] ID = Bytes.toBytes("ID");
    private static final byte[] XMJC = Bytes.toBytes("XMJC");
    private static final byte[] BDBH = Bytes.toBytes("BDBH");
    private static final byte[] QDBH = Bytes.toBytes("QDBH");


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String rowkey = "";
        String[] fields = value.toString().split(";");
        if(fields.length!=6){
            System.out.println("数据有误！");//避免数组下标越界
        }else{
            String key1 = new StringBuffer().append(fields[1]).reverse().toString();
            rowkey =key1+"|"+fields[4]+"|"+fields[0];
        }
        ImmutableBytesWritable rowKey = new ImmutableBytesWritable(rowkey.getBytes());


        Put put = new Put(Bytes.toBytes(rowkey));   //根据rowkey创建Put对象
        put.addColumn(COLUNMFAMILY,ID,fields[0].getBytes());
        put.addColumn(COLUNMFAMILY, GCXM, fields[1].getBytes());
        put.addColumn(COLUNMFAMILY, XMMC, fields[2].getBytes());
        put.addColumn(COLUNMFAMILY, XMJC, fields[3].getBytes());
        put.addColumn(COLUNMFAMILY, BDBH, fields[4].getBytes());
        put.addColumn(COLUNMFAMILY, QDBH, fields[5].getBytes());

        context.write(rowKey, put);
    }
}