package ecnu.db.transaction;

import ecnu.db.utils.MysqlConnector;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Random;

public abstract class BaseTransaction {
    protected Random r = new Random();
    protected MysqlConnector mysqlConnector;
    boolean isSelect;
    PreparedStatement preparedOutStatement;
    PreparedStatement preparedInStatement;
    PreparedStatement preparedOutSelectStatement;
    PreparedStatement preparedInSelectStatement;

    BaseTransaction(MysqlConnector mysqlConnector, boolean isSelect) {
        this.isSelect = isSelect;
        this.mysqlConnector = mysqlConnector;
        mysqlConnector.beginTransaction();
    }

    /**
     * 执行本类事务
     *
     * @throws SQLException 事务执行时和数据库交互失败本事务会报错
     */
    public abstract void execute() throws SQLException;
}
