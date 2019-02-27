package ecnu.db.transaction;

import ecnu.db.utils.MysqlConnector;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Random;

public abstract class BaseTransaction {
    Connection conn;
    boolean isSelect;
    protected Random r = new Random();
    PreparedStatement preparedOutStatement;
    PreparedStatement preparedInStatement;
    PreparedStatement preparedOutSelectStatement;
    PreparedStatement preparedInSelectStatement;
    BaseTransaction(MysqlConnector mysqlConnector,boolean isSelect){
        this.conn=mysqlConnector.getConn();
        this.isSelect=isSelect;
    }
    public abstract void execute();
}
