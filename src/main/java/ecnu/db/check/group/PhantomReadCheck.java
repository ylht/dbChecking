package ecnu.db.check.group;

import ecnu.db.check.BaseCheck;
import ecnu.db.transaction.PhantomRead;
import ecnu.db.utils.MysqlConnector;

import java.sql.SQLException;

public class PhantomReadCheck extends BaseCheck {
    private int errCount;

    public PhantomReadCheck() {
        super("SampleConfig.xml");
    }

    @Override
    public void makeTransaction() {
        transaction=new PhantomRead(workNodes.get(0),config.getSleepMills(),config.getReadWriteRadio());
    }

    @Override
    public void recordBeginStatus(MysqlConnector mysqlConnector) {

    }

    @Override
    public void recordEndStatus(MysqlConnector mysqlConnector) throws SQLException {
        errCount = mysqlConnector.getPhantomRecordNum();
    }

    @Override
    public boolean checkCorrect() {
        return errCount == 0;
    }
}
