package ecnu.db.utils;

import ecnu.db.check.WorkNode;
import ecnu.db.config.TableConfig;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashSet;

/**
 * @author wangqingshuai
 * 数据库驱动连接器
 */
public class MysqlConnector {
    /**
     * JDBC 驱动名及数据库 URL
     */
    private Connection conn;

    public MysqlConnector() {

        String dbUrl = "jdbc:mysql://biui.me/databaseChecking?useSSL=false&allowPublicKeyRetrieval=true";

        // 数据库的用户名与密码，需要根据自己的设置
        String user = "qswang";
        String pass = "Biui1227..";

        try {
            conn = DriverManager.getConnection(dbUrl, user, pass);
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println("无法建立数据库连接");
            System.exit(-1);
        }
    }

    public static void main(String[] args) throws SQLException {
        MysqlConnector mysqlConnector = new MysqlConnector();
        System.out.println(mysqlConnector.sumColumn(0, 1));
    }

    //数据库标准操作

    public void beginTransaction() {
        try {
            conn.setAutoCommit(false);
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println("无法进入事务模式");
            System.exit(-1);
        }
    }

    public void commit() throws SQLException {
        conn.commit();
    }

    public void rollback() throws SQLException {
        conn.rollback();
    }

    public void close() {
        try {
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public PreparedStatement getPrepareStatement(String sql) throws SQLException {
        return conn.prepareStatement(sql);
    }

    public void executeSql(String sql) throws SQLException {
        conn.createStatement().execute(sql);
    }

    //表格相关操作

    public void loadData(int tableIndex) throws SQLException {
        executeSql("SET FOREIGN_KEY_CHECKS = 0;");
        String sql = "load data CONCURRENT LOCAL INFILE 'data/t" + tableIndex +
                ".csv' into table t" + tableIndex + " COLUMNS TERMINATED BY ',' ";
        executeSql(sql);
        executeSql("SET FOREIGN_KEY_CHECKS = 1;");
    }

    /**
     * 在本项目中表的命名都用t开头，因此我们从t0开始删除指定数量的表，
     * 来进行本次执行的初始化
     */
    public void dropTables() throws SQLException {
        String sql = "DROP TABLE IF EXISTS t";
        int max = TableConfig.getConfig().getMaxTableNum();
        for (int i = max; i >= 0; i--) {
            executeSql(sql + i);
        }
        sql = "DROP TABLE IF EXISTS order_item";
        executeSql(sql);
        sql = "DROP TABLE IF EXISTS phantom_read_record";
        executeSql(sql);
    }

    public void createOrderTable() throws SQLException {
        String sql = "CREATE TABLE order_item (tableIndex INT,tupleIndex INT,num INT default 0)";
        executeSql(sql);
    }

    public void createPhantomReadRecordTable() throws SQLException {
        String sql = "CREATE TABLE phantom_read_record (tableIndex int,type int)";
        executeSql(sql);
    }


    //验证语句

    public int getPhantomRecordNum() throws SQLException {
        String sql = "select count(*) from phantom_read_record";
        ResultSet rs = conn.createStatement().executeQuery(sql);
        rs.next();
        return rs.getInt(1);
    }

    public int getNoCommitCount(int tableIndex) throws SQLException {
        String sql = "select count(*) from t" + tableIndex + " where checkReadCommitted<0";
        ResultSet rs = conn.createStatement().executeQuery(sql);
        rs.next();
        return rs.getInt(1);
    }

    public int getSumRepeatableRead(int tableIndex) throws SQLException {
        String sql = "select count(*) from t" + tableIndex + " where checkRepeatableRead!=0";
        ResultSet rs = conn.createStatement().executeQuery(sql);
        rs.next();
        return rs.getInt(1);
    }

    public int getWriteSkewResult(ArrayList<WorkNode> workNodes) throws SQLException {
        String tableName="t"+workNodes.get(0).getTableIndex();
        String columnName1="tp"+workNodes.get(0).getColumnIndex();
        String columnName2="tp"+workNodes.get(1).getColumnIndex();
        ResultSet rs = conn.createStatement().executeQuery("select count(*) from " + tableName + " where " + columnName1 + " + " + columnName2 + " < 0 ;");
        rs.next();
        return rs.getInt(1);

    }

    public int getOrderItem(int tableIndex, int tupleIndex) throws SQLException {
        String sql = "select sum(num) from order_item" +
                " where tableIndex=" + tableIndex + " and tupleIndex =" + tupleIndex;
        ResultSet rs = conn.createStatement().executeQuery(sql);
        rs.next();
        return rs.getInt(1);
    }

    public Double sumColumn(int tableIndex, int tupleIndex) throws SQLException {
        String tableName = "t" + tableIndex;
        String tupleName = "tp" + tupleIndex;
        String sql = "select sum(" + tupleName + ") from " + tableName;
        ResultSet rs = conn.createStatement().executeQuery(sql);
        rs.next();
        return rs.getDouble(1);
    }
}
