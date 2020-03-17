package com.diao.beans;

import com.diao.constant.ValueConst;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


/**
 * 基础数据访问（HBase）对象
 */
public abstract class BaseDao {
    private ThreadLocal<Connection> coonHolder = new ThreadLocal<Connection>();
    private ThreadLocal<Admin> adminHolder = new ThreadLocal<Admin>();

    protected void start(){
        try{
            //获取线程中的连接对象
            Connection connection = coonHolder.get();
            if(connection==null){
                //创建默认配置对象
                Configuration conf = HBaseConfiguration.create();
                //获取连接对象
                connection = ConnectionFactory.createConnection(conf);
                //将连接对象放入缓存
                coonHolder.set(connection);
            }

            Admin admin = adminHolder.get();
            if (admin==null){
                admin = connection.getAdmin();
                adminHolder.set(admin);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    protected void end() throws Exception {
        Admin admin = adminHolder.get();
        if(admin!=null){
            admin.close();
            adminHolder.remove();
        }
        Connection connection = coonHolder.get();
        if (connection!=null){
            connection.close();
            coonHolder.remove();
        }
    }

    /**
     * 创建命名空间，如果不存在，创建新的，否则什么都不做
     * @param nameSpace
     */
    protected void createNameSpace(String nameSpace) throws IOException {

        Admin admin = adminHolder.get();

        try{
            NamespaceDescriptor namespaceDescriptor = admin.getNamespaceDescriptor(nameSpace);
        }catch (Exception e ){
            e.printStackTrace();
            //创建命名空间，如果不存在直接抛异常，所以不判断namespaceDescriptor是否为null
            NamespaceDescriptor namespaceDescriptore = NamespaceDescriptor.create(nameSpace).build();
            admin.createNamespace(namespaceDescriptore);
        }
    }

    /**
     * 创建表，如果存在，删除后创建，否则直接创建
     * @param tableName
     */

    protected void createTable(String tableName,String coprocessorClassName,String... columnFamilies) throws Exception{
       createTableXX(tableName,coprocessorClassName, ValueConst.REGION_DEFAULT_COUNT,columnFamilies);
    }

    protected void createTableXX(String tableName,String clazzName,int regionCount,String... columnFamilies)throws Exception{
        
        Admin admin = adminHolder.get();
        TableName tableName1 = TableName.valueOf(tableName);
        //判断表是否存在
        if(admin.tableExists(tableName1)){
            admin.disableTable(tableName1);
            admin.deleteTable(tableName1);
        }
        //创建表描述器
        HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName1);
        //创建表
        if(columnFamilies != null && columnFamilies.length!=0){
            for (String columnFamily : columnFamilies) {
                hTableDescriptor.addFamily(new HColumnDescriptor(columnFamily));
            }
        }
        //添加协处理器
        if(clazzName !=null && !"".equals(clazzName.trim())){
            hTableDescriptor.addCoprocessor(clazzName);
        }

        //使用分区键创建分区
        admin.createTable(hTableDescriptor,getSplitKeys(regionCount));
    }

    /**
     * 生成分区键
     * @param regionCount 分区数
     * @param splitKeys 指定分区键
     * @return
     */
    private byte[][] getSplitKeys(int regionCount,String...splitKeys){
        int splitKeySize = regionCount -1;
        if(splitKeySize!=splitKeys.length){
            System.out.println("输入分区键数量与分区数不符，请重新输入！（提示：键数为分区数-1）");
        }
        byte[][] keys = new byte[splitKeySize][];
        //将分区键放入集合进行排序
        List<byte[]> splitKeyList = new ArrayList<byte[]>();
        for (String splitKey : splitKeys) {
            splitKeyList.add(Bytes.toBytes(splitKey+"|"));
        }
        //定制排序规则
        // splitKeyList.sort();
        splitKeyList.toArray(keys);
        return keys;

    }


}
