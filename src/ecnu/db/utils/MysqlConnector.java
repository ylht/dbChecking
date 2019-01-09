package ecnu.db.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;


public class MysqlConnector {
    /** JDBC 驱动名及数据库 URL*/
    private Connection conn;

    private Statement stmt;
    public MysqlConnector() {
        try {
            String dbUrl = "jdbc:mysql://111.231.50.91:3306/accDB?useSSL=false";

            // 数据库的用户名与密码，需要根据自己的设置
            String user = "sqxdz";
            String pass = "Biui1227..";

            conn = DriverManager.getConnection(dbUrl, user, pass);
            stmt = conn.createStatement();
        } catch (Exception e) {
            System.out.println(e.getMessage());
            System.exit(-1);
        }
    }


    public Boolean excuteSql(String sql) {
        try {
            stmt.execute(sql);
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println(sql);
            System.exit(-1);
            return false;
        }
    }

    public Connection getConn() {
        try {
            conn.setAutoCommit(false);
        } catch (SQLException se) {
            System.out.println(se.getErrorCode());
            System.exit(-1);
        }
        return conn;
    }
}
