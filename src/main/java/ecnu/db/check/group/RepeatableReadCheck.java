package ecnu.db.check.group;

import ecnu.db.check.BaseCheck;
import ecnu.db.transaction.RepeatableRead;
import ecnu.db.utils.MysqlConnector;

import java.sql.SQLException;

public class RepeatableReadCheck extends BaseCheck {
    private int errCount;

    public RepeatableReadCheck() {
        super("RepeatableReadConfig.xml");
    }

    @Override
    public void makeTransaction() {
        transaction = new RepeatableRead(workNodes.get(0), config.getSleepMills(), config.getReadWriteRadio());
    }

    @Override
    public void recordBeginStatus(MysqlConnector mysqlConnector) {

    }

    @Override
    public void recordEndStatus(MysqlConnector mysqlConnector) throws SQLException {
        errCount = mysqlConnector.getSumRepeatableRead(workNodes.get(0).getTableIndex());
    }


    @Override
    public boolean checkCorrect() {
        return errCount == 0;
    }
}
