package ecnu.db.transaction;

import ecnu.db.scheme.AbstractColumn;
import ecnu.db.utils.MysqlConnector;

import java.sql.SQLException;
import java.util.Random;

public abstract class BaseTransaction implements Cloneable {
    protected final Random R = new Random();
    MysqlConnector mysqlConnector;
    AbstractColumn.ColumnType columnType;

    /**
     * 将本地的SQL语句构建为PrepareStatement
     *
     * @param mysqlConnector 用于构建PrepareStatement
     * @throws SQLException 数据库连接错误，或者SQL语法出错
     */
    public abstract void makePrepareStatement(MysqlConnector mysqlConnector) throws SQLException;

    /**
     * 执行本类事务
     *
     * @throws SQLException 事务执行时和数据库交互失败本事务会报错
     */
    public abstract void execute() throws SQLException;

    @Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }
}
