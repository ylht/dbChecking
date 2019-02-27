package ecnu.db.transaction;

import ecnu.db.utils.MysqlConnector;

import java.sql.Connection;
import java.util.Random;

public abstract class BaseTransaction {
    protected Connection conn;
    protected Random r = new Random();
    public BaseTransaction(MysqlConnector mysqlConnector){
        this.conn=mysqlConnector.getConn();
    }
    public abstract void execute();
}
