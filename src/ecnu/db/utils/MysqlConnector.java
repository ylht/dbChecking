package ecnu.db.utils;

import java.sql.*;

/**
 * @author wangqingshuai
 * 数据库驱动连接器
 */
public class MysqlConnector {
    /**
     * JDBC 驱动名及数据库 URL
     */
    private Connection conn;
    private Statement stmt;

    public MysqlConnector() {
        try {
            String dbUrl = "jdbc:mysql://10.11.1.193:13306/qswang_dbchecking?useSSL=false";

            // 数据库的用户名与密码，需要根据自己的设置
            String user = "root";
            String pass = "root";

            conn = DriverManager.getConnection(dbUrl, user, pass);
            stmt = conn.createStatement();
        } catch (Exception e) {
            System.out.println(e.getMessage());
            System.exit(-1);
        }
    }

    public static void main(String[] args) {
        MysqlConnector mysqlConnector = new MysqlConnector();
        System.out.println(mysqlConnector.sumColumn(0, 1));
    }

    public Connection getConn() {
        return conn;
    }

    public void excuteSql(String sql) {
        try {
            stmt.execute(sql);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println(sql);
            System.exit(-1);
        }
    }

    /**
     * 在本项目中表的命名都用t开头，因此我们从t0开始删除指定数量的表，
     * 来进行本次执行的初始化
     *
     * @param num 需要删除的表的数量
     */
    public void dropTables(int num) {
        String sql = "DROP TABLE IF EXISTS t";
        for (int i = 0; i < num; i++) {
            excuteSql(sql + i);
        }
    }

    public PreparedStatement getPrepareUpdate(boolean add, int tableIndex, int tupleIndex) {
        String tableName = "t" + tableIndex;
        String tupleName = "tp" + tupleIndex;
        String sql = "update " + tableName + " set " + tupleName + "=" + tupleName;
        if (add) {
            sql += "+";
        } else {
            sql += "-";
        }
        sql += " ? where tp0 = ? and tp" + tupleIndex;
        if (add) {
            sql += " < ?";
        } else {
            sql += " > ?";
        }
        try {
            return conn.prepareStatement(sql);
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    public void loadData(int tableIndex) {
        String sql = "load data local infile 'randomData/t" + tableIndex +
                "' replace into table t" + tableIndex + " columns terminated by ',' ";
        excuteSql(sql);
    }

    public Double sumColumn(int tableIndex, int tupleIndex) {
        String tableName = "t" + tableIndex;
        String tupleName = "tp" + tupleIndex;
        String sql = "select sum(" + tupleName + ") from " + tableName;
        try {
            ResultSet rs = stmt.executeQuery(sql);
            rs.next();
            return rs.getDouble(1);
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    public void close() {
        try {
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
