package ecnu.db.utils;

import org.apache.logging.log4j.LogManager;

import java.sql.*;
import java.util.ArrayList;

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
            String dbUrl = "jdbc:mysql://10.11.1.193:13306/databaseChecking?useSSL=false";

            // 数据库的用户名与密码，需要根据自己的设置
            String user = "root";
            String pass = "root";

            conn = DriverManager.getConnection(dbUrl, user, pass);
            stmt = conn.createStatement();
        } catch (Exception e) {
            LogManager.getLogger().error(e);
            System.exit(-1);
        }
    }

    public static void main(String[] args) {
        MysqlConnector mysqlConnector = new MysqlConnector();
        System.out.println(mysqlConnector.sumColumn(0, 1));
    }

    public Connection getConn() {
        try {
            conn.setAutoCommit(false);
        } catch (SQLException e) {
            LogManager.getLogger().error(e);
        }
        return conn;
    }

    public void executeSql(String sql) {
        try {
            stmt.execute(sql);
        } catch (Exception e) {
            LogManager.getLogger().error(e);
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
            executeSql(sql + i);
        }
    }

    public PreparedStatement getSelect(boolean forUpdate, int tableIndex, int tupleIndex) {
        String tableName = "t" + tableIndex;
        String tupleName = "tp" + tupleIndex;
        String sql = "select " + tupleName + " from " + tableName;
        sql += " where tp0 =?";
        if (forUpdate) {
            sql += " for update";
        }
        try {
            return conn.prepareStatement(sql);
        } catch (SQLException e) {
            LogManager.getLogger().error(e);
            return null;
        }
    }

    public PreparedStatement getOrderUpdate(boolean add, int tableIndex, int tupleIndex, boolean forSelect) {
        String tableName = "t" + tableIndex;
        String tupleName = "tp" + tupleIndex;
        String sql = "update " + tableName + " set " + tupleName + "=";
        if (forSelect) {
            sql += " ?";
        } else {
            sql += tupleName;
        }
        if (add) {
            sql += "+1";
        } else {
            sql += "-1";
        }
        sql += " where tp0=?";
        if (!add) {
            sql += " and " + tupleName + ">0";
        }
        try {
            return conn.prepareStatement(sql);
        } catch (SQLException e) {
            LogManager.getLogger().error(e);
            return null;
        }
    }

    public PreparedStatement getDeleteStatement(int tableIndex) {
        String tableName = "t" + tableIndex;
        String keyName = "tp0";
        String sql = "delete from " + tableName + " where " + keyName + " =?";
        try {
            return conn.prepareStatement(sql);
        } catch (SQLException e) {
            LogManager.getLogger().error(e);
            return null;
        }
    }

    public PreparedStatement getUpdateNoCommitStatement(int tableIndex, int tupleIndex) {
        String tableName = "t" + tableIndex;
        String tupleName = "tp" + tupleIndex;
        String keyName="tp0";
        String sql = "update " + tableName + " set " + keyName + "= - " + keyName + " where " + tupleName + " between ? and ?";
        try {
            return conn.prepareStatement(sql);
        } catch (SQLException e) {
            LogManager.getLogger().error(e);
            return null;
        }
    }

    public PreparedStatement getInsertStatement(int tableIndex, int valueNum) {
        String tableName = "t" + tableIndex;
        StringBuilder sql = new StringBuilder("replace into " + tableName + " values(?");
        for (int i = 0; i < valueNum; i++) {
            sql.append(",?");
        }
        sql.append(")");
        try {
            return conn.prepareStatement(sql.toString());
        } catch (SQLException e) {
            LogManager.getLogger().error(e);
            return null;
        }
    }

    public PreparedStatement getScanStatement(int tableIndex, int tupleIndex) {
        String tableName = "t" + tableIndex;
        String tupleName = "tp" + tupleIndex;
        String keyName = "tp0";
        String sql = "select " + keyName + " from " + tableName + " where " + tupleName + " between ? and ?";
        try {
            return conn.prepareStatement(sql);
        } catch (SQLException e) {
            LogManager.getLogger().error(e);
            return null;
        }
    }

    //将range范围内的所有数据加1
    public PreparedStatement getUpdateAllStatement(int tableIndex, int tupleIndex) {
        String tableName = "t" + tableIndex;
        String tupleName = "tp" + tupleIndex;
        String sql = "update " + tableName + " set " + tupleName + "=" + tupleName + " +1 where " + tupleName + " between ? and ?";
        try {
            return conn.prepareStatement(sql);
        } catch (SQLException e) {
            LogManager.getLogger().error(e);
            return null;
        }
    }

    public PreparedStatement getRemittanceUpdate(boolean add, int tableIndex, int tupleIndex, boolean forSelect) {
        String tableName = "t" + tableIndex;
        String tupleName = "tp" + tupleIndex;
        String sql = " update " + tableName + " set " + tupleName + "=";
        if (forSelect) {
            sql += " ? ";
        } else {
            sql += tupleName;
        }
        if (add) {
            sql += "+";
        } else {
            sql += "-";
        }
        sql += "? where tp0 = ? and tp" + tupleIndex;
        if (add) {
            sql += " < ?";
        } else {
            sql += " > ?";
        }
        try {
            return conn.prepareStatement(sql);
        } catch (SQLException e) {
            LogManager.getLogger().error(e);
            return null;
        }
    }

    public void loadData(int tableIndex) {
        String sql = "load data local infile 'data/t" + tableIndex +
                "' replace into table t" + tableIndex + " columns terminated by ',' ";
        executeSql(sql);
    }

    public Double[] getTableData(int tableIndex, int tupleIndex) {
        String tableName = "t" + tableIndex;
        String tupleName = "tp" + tupleIndex;
        String sql = "select " + tupleName + " from " + tableName;
        ArrayList<Double> datas = new ArrayList<>();
        try {
            ResultSet rs = stmt.executeQuery(sql);
            while (rs.next()) {
                datas.add(rs.getDouble(1));
            }
            return datas.toArray(new Double[0]);
        } catch (SQLException e) {
            LogManager.getLogger().error(e);
            //System.out.println(e.getSQLState());
            return null;
        }
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
            LogManager.getLogger().error(e);
            return null;
        }
    }

    public void close() {
        try {
            conn.close();
        } catch (SQLException e) {
            LogManager.getLogger().error(e);
        }
    }
}
