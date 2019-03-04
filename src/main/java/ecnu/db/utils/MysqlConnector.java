package ecnu.db.utils;

import ecnu.db.work.group.WorkNode;

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
    private Statement stmt;

    public MysqlConnector() {

        String dbUrl = "jdbc:mysql://10.11.1.193:13306/databaseChecking?useSSL=false";

        // 数据库的用户名与密码，需要根据自己的设置
        String user = "root";
        String pass = "root";

        try {
            conn = DriverManager.getConnection(dbUrl, user, pass);
            stmt = conn.createStatement();
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

    public void executeSql(String sql) throws SQLException {
        stmt.execute(sql);
    }

    //表格相关操作

    public void loadData(int tableIndex) throws SQLException {
        String sql = "load data local infile 'data/t" + tableIndex +
                "' replace into table t" + tableIndex + " columns terminated by ',' ";
        executeSql(sql);
    }

    /**
     * 在本项目中表的命名都用t开头，因此我们从t0开始删除指定数量的表，
     * 来进行本次执行的初始化
     *
     * @param num 需要删除的表的数量
     */
    public void dropTables(int num) throws SQLException {
        String sql = "DROP TABLE IF EXISTS t";
        for (int i = 0; i < num; i++) {
            executeSql(sql + i);
        }
        sql = "DROP TABLE IF EXISTS order_item";
        executeSql(sql);
        sql="DROP TABLE IF EXISTS phantom_read_record";
        executeSql(sql);
    }

    public void createOrderTable() throws SQLException {
        String sql = "CREATE TABLE order_item (tableIndex INT,tupleIndex INT)";
        executeSql(sql);
    }

    public void createPhantomReadRecordTable() throws SQLException {
        String sql = "CREATE TABLE phantom_read_record (tableIndex int,type int)";
        executeSql(sql);
    }


    //基本事务语句

    public PreparedStatement getSelect(boolean forUpdate, int tableIndex, int tupleIndex) throws SQLException {
        String tableName = "t" + tableIndex;
        String tupleName = "tp" + tupleIndex;
        String sql = "select " + tupleName + " from " + tableName;
        sql += " where tp0 =?";
        if (forUpdate) {
            sql += " for update";
        }
        return conn.prepareStatement(sql);
    }

    public PreparedStatement getRemittanceUpdate(boolean add, int tableIndex,
                                                 int tupleIndex, boolean forSelect) throws SQLException {
        String tableName = "t" + tableIndex;
        String tupleName = "tp" + tupleIndex;
        String sql = " update " + tableName + " set " + tupleName + "=";
        if (!forSelect) {
            if (add) {
                sql += tupleName + "+";
            } else {
                sql += tupleName + "-";
            }
        }
        sql += "? where tp0 = ? and " + tupleName;
        if (!add) {
            sql += " > ?";
        }

        return conn.prepareStatement(sql);

    }

    public PreparedStatement getOrderUpdate(int tableIndex, int tupleIndex, boolean forSelect) throws SQLException {
        String tableName = "t" + tableIndex;
        String tupleName = "tp" + tupleIndex;
        String sql = "update " + tableName + " set " + tupleName + "=";
        if (forSelect) {
            sql += " ?";
        } else {
            sql += tupleName + "-1";
        }
        sql += " where tp0=? and " + tupleName + ">0";
        return conn.prepareStatement(sql);
    }

    public PreparedStatement insertOrderItem(int tableIndex,int tupleIndex) throws SQLException {
        String sql="insert into order_item values ("+tableIndex+","+tupleIndex+")";
        return conn.prepareStatement(sql);
    }


    public PreparedStatement getWriteSkewUpdate(int tableIndex, int tupleIndex) throws SQLException {
        String tableName = "t" + tableIndex;
        String tupleName = "tp" + tupleIndex;
        String keyName = "tp0";
        String sql = " update " + tableName +
                " set " + tupleName + "=" + tupleName + "-?" +
                " where " + keyName + "=?";
        return conn.prepareStatement(sql);
    }

    //读已提交事务语句

    public PreparedStatement getUpdateNoCommitStatement(int tableIndex, int tupleIndex) throws SQLException {
        String tableName = "t" + tableIndex;
        String tupleName = "tp" + tupleIndex;
        String keyName = "tp0";
        String sql = "update " + tableName +
                " set " + tupleName + "= - " + tupleName +
                " where " + keyName + " between ? and ?";

        return conn.prepareStatement(sql);
    }

    public PreparedStatement getInsertNoCommitColStatement(int tableIndex) throws SQLException{
        String sql="update t"+tableIndex+" set checkNoCommit =? where tp0 =? and checkNoCommit>=0";
        return conn.prepareStatement(sql);
    }

    //可重复读事务语句

    public PreparedStatement getInsertRepeatableReadColStatement(int tableIndex) throws SQLException {
        String sql="update t"+tableIndex+" set checkRepeatableRead =checkRepeatableRead+? where tp0 =? and checkNoCommit>=0";
        return conn.prepareStatement(sql);
    }

    //幻读事务语句

    public PreparedStatement getScanStatement(int tableIndex, int tupleIndex) throws SQLException {
        String tableName = "t" + tableIndex;
        String tupleName = "tp" + tupleIndex;
        String keyName = "tp0";
        String sql = "select " + keyName + " from " + tableName +
                " where " + tupleName + " between ? and ?";
        return conn.prepareStatement(sql);
    }

    public PreparedStatement getInsertStatement(int tableIndex, int valueNum) throws SQLException {
        String tableName = "t" + tableIndex;
        StringBuilder sql = new StringBuilder("replace into " + tableName + " values(?");
        for (int i = 0; i < valueNum; i++) {
            sql.append(",?");
        }
        sql.append(")");
        return conn.prepareStatement(sql.toString());
    }

    public PreparedStatement getDeleteStatement(int tableIndex) throws SQLException {
        String tableName = "t" + tableIndex;
        String keyName = "tp0";
        String sql = "delete from " + tableName + " where " + keyName + " =?";

        return conn.prepareStatement(sql);

    }

    /**
     * 将range范围内的所有数据加1
     */
    public PreparedStatement getUpdateAllStatement(int tableIndex, int tupleIndex) throws SQLException {
        String tableName = "t" + tableIndex;
        String tupleName = "tp" + tupleIndex;
        String sql = "update " + tableName + " set " + tupleName + "=" + tupleName + " +1" +
                " where " + tupleName + " between ? and ?";
        return conn.prepareStatement(sql);
    }

    public PreparedStatement getInsertPhantomReadRecordStatement(int tableIndex) throws SQLException {
        String sql="insert into phantom_read_record values("+tableIndex+",?)";
        return conn.prepareStatement(sql);
    }


    //验证语句

    public int getPhantomRecordNum() throws SQLException {
        String sql="select count(*) from phantom_read_record";
        ResultSet rs = stmt.executeQuery(sql);
        rs.next();
        return rs.getInt(1);
    }

    public int getNoCommitCount(int tableIndex) throws SQLException {
        String sql="select count(*) from t"+tableIndex+" where checkNoCommit<0";
        ResultSet rs=stmt.executeQuery(sql);
        rs.next();
        return rs.getInt(1);
    }

    public int getSumRepeatableRead(int tableIndex) throws SQLException {
        String sql="select count(*) from t"+tableIndex+" where checkRepeatableRead!=0";
        ResultSet rs=stmt.executeQuery(sql);
        rs.next();
        return rs.getInt(1);
    }

    public int getWriteSkewResult(ArrayList<WorkNode> workNodes) throws SQLException {
        StringBuilder sql = new StringBuilder("select count(*) from ");
        HashSet<Integer> allTable = new HashSet<>();
        for (WorkNode workNode : workNodes) {
            allTable.add(workNode.getTableIndex());
        }
        for (Integer integer : allTable) {
            sql.append("t").append(integer).append(",");
        }
        sql.deleteCharAt(sql.length() - 1).append(" where ");
        if (allTable.size() > 1) {
            Integer[] tableIndex = allTable.toArray(new Integer[0]);
            String firstTable = "t" + tableIndex[0] + ".tp0=";
            for (int i = 1; i < tableIndex.length; i++) {
                sql.append(firstTable).append('t').append(tableIndex[i])
                        .append(".tp0").append(" and ");
            }
        }
        for (WorkNode workNode : workNodes) {
            sql.append('t').append(workNode.getTableIndex()).append('.').
                    append("tp").append(workNode.getTupleIndex()).append('+');
        }
        sql.deleteCharAt(sql.length() - 1).append("<0");

        ResultSet rs = stmt.executeQuery(sql.toString());
        rs.next();
        return rs.getInt(1);

    }

    public int getOrderItem(int tableIndex,int tupleIndex) throws SQLException {
        String sql="select count(*) from order_item" +
                " where tableIndex="+tableIndex+" and tupleIndex ="+tupleIndex;
        ResultSet rs = stmt.executeQuery(sql);
        rs.next();
        return rs.getInt(1);
    }

    public Double sumColumn(int tableIndex, int tupleIndex) throws SQLException {
        String tableName = "t" + tableIndex;
        String tupleName = "tp" + tupleIndex;
        String sql = "select sum(" + tupleName + ") from " + tableName;
        ResultSet rs = stmt.executeQuery(sql);
        rs.next();
        return rs.getDouble(1);
    }
}
