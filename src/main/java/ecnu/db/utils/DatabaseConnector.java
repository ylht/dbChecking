package ecnu.db.utils;

import ecnu.db.config.SystemConfig;
import ecnu.db.config.TableConfig;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;

/**
 * @author wangqingshuai
 * 数据库驱动连接器
 */
public class DatabaseConnector {
    /**
     * JDBC 驱动名及数据库 URL
     */
    private Connection conn;

    public DatabaseConnector() {
        String dbUrl = null;
        switch (SystemConfig.getConfig().getDatabaseVersion()) {
            case "mysql":
                dbUrl = "jdbc:mysql://" + SystemConfig.getConfig().getDatabaseurl() + "/" +
                        SystemConfig.getConfig().getDatabaseName() +
                        "?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC";
                break;
            case "postgresql":
                dbUrl = "jdbc:postgresql://" + SystemConfig.getConfig().getDatabaseurl() + "/" +
                        SystemConfig.getConfig().getDatabaseName();
                break;
            default:
                System.out.println("配置文件错误");
                System.exit(-1);
        }

        // 数据库的用户名与密码
        String user = SystemConfig.getConfig().getDatabaseUser();
        String pass = SystemConfig.getConfig().getDatabasePassword();
        try {
            conn = DriverManager.getConnection(dbUrl, user, pass);
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println("无法建立数据库连接");
            System.exit(-1);
        }
    }

    public static void main(String[] args) throws SQLException {
        DatabaseConnector databaseConnector = new DatabaseConnector();
        System.out.println(databaseConnector.sumColumn(0, 1));
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
        return conn.prepareStatement(sql,ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
    }

    public void executeSql(String sql) throws SQLException {
        conn.createStatement().execute(sql);
    }

    //表格相关操作

    void loadData(int tableIndex) throws SQLException {
        switch (SystemConfig.getConfig().getDatabaseVersion()) {
            case "mysql":
                executeSql("SET FOREIGN_KEY_CHECKS = 0;");
                String sql = "load data CONCURRENT LOCAL INFILE 'data/t" + tableIndex +
                        ".csv' into table t" + tableIndex + " COLUMNS TERMINATED BY ',' ";
                executeSql(sql);
                executeSql("SET FOREIGN_KEY_CHECKS = 1;");
                break;
            case "postgresql":
                CopyManager copyManager = new CopyManager((BaseConnection) conn);
                try {
                    copyManager.copyIn("COPY t" + tableIndex + " FROM stdin DELIMITER as ',';",
                            new FileReader(new File("data/t" + tableIndex+".csv")));
                } catch (IOException e) {
                    e.printStackTrace();
                }
                break;
            default:
                System.out.println("配置文件错误");
                System.exit(-1);
        }

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

    public int getResult(String testSQL) throws SQLException {
        ResultSet rs = conn.createStatement().executeQuery(testSQL);
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
