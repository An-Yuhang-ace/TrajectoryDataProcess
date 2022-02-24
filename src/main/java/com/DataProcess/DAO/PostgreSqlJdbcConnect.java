package com.DataProcess.DAO;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * TODO
 *
 * @author An Yuhang
 * @version 1.0
 * @date 2021/10/26 11:28
 */
public class PostgreSqlJdbcConnect {
    public static Connection databaseConnection;

    public static Statement creatStatement(){
        Connection c = null;
        Statement s = null;
        try{
            Class.forName("org.postgresql.Driver");
            c = DriverManager.getConnection("jdbc:postgresql://172.17.61.61:5432/gis", "postgres", "123456");
            s = c.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        }catch (Exception e){
            e.printStackTrace();
            System.err.println(e.getClass().getName()+": "+e.getMessage());
            System.exit(0);
        }
        System.out.println("Opened database successfully");
        return s;
    }

    public static void main(String[] args){
        Connection c = null;
        try{
            Class.forName("org.postgresql.Driver");
            c = DriverManager.getConnection("jdbc:postgresql://172.17.61.61:5432/gis", "postgres", "123456");
        }catch (Exception e){
            e.printStackTrace();
            System.err.println(e.getClass().getName()+": "+e.getMessage());
            System.exit(0);
        }
        System.out.println("Opened database successfully");
    }

}
