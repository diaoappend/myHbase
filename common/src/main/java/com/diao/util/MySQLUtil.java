package com.diao.util;

import com.diao.constant.MySQLConst;

import java.sql.Connection;
import java.sql.DriverManager;

public class MySQLUtil {
    public static Connection getConnection(String baseName){
        Connection conn = null;

        try{
            Class.forName(MySQLConst.MYSQL_DRIVER_CLASS);
            conn= DriverManager.getConnection(MySQLConst.MYSQL_URL+baseName+"?useUnicode=true&characterEncoding=utf8",MySQLConst.MYSQL_USERNAME,MySQLConst.MYSQL_PASSWORD);
        }catch(Exception e){
            e.printStackTrace();
        }

        return conn;
    }
}
