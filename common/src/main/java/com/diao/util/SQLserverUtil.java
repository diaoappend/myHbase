package com.diao.util;

import com.diao.constant.SQLServerConst;

import java.sql.Connection;
import java.sql.DriverManager;

public class SQLserverUtil {
    public static Connection getConnection(String baseName){
        Connection conn = null;

        try{
            Class.forName(SQLServerConst.SQLSERVER_DRIVER_CLASS);
            conn= DriverManager.getConnection(SQLServerConst.SQLSERVER_URL+baseName,SQLServerConst.SQLSERVER_USERNAME,SQLServerConst.SQLSERVER_PASSWORD);
        }catch(Exception e){
            e.printStackTrace();
        }

        return conn;
    }
}
