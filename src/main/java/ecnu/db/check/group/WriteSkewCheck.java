package ecnu.db.check.group;

import ecnu.db.check.BaseCheck;
import ecnu.db.transaction.WriteSkew;
import ecnu.db.utils.MysqlConnector;

import java.sql.SQLException;

//todo 考虑如何针对该事务引入同表的列进行操作

public class WriteSkewCheck extends BaseCheck {
    private int errCount;


    public WriteSkewCheck() {
        super("WriteSkewConfig.xml");
    }

    @Override
    public void makeTransaction() {
        transaction = new WriteSkew(workNodes);
    }

    @Override
    public void recordBeginStatus(MysqlConnector mysqlConnector) {
    }

    @Override
    public void recordEndStatus(MysqlConnector mysqlConnector) throws SQLException {
        errCount = mysqlConnector.getWriteSkewResult(workNodes);
    }

    @Override
    public boolean checkCorrect() {
        return errCount == 0;
    }
}
