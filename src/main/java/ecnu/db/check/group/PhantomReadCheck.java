package ecnu.db.check.group;

import ecnu.db.check.BaseCheck;
import ecnu.db.transaction.PhantomRead;
import ecnu.db.utils.MysqlConnector;

import java.sql.SQLException;

public class PhantomReadCheck extends BaseCheck {
    private int errCount;

    public PhantomReadCheck() {
        super("PhantomReadConfig.xml");
    }

    @Override
    public void makeTransaction() {
        transaction = new PhantomRead(workNodes.get(0), config.getSleepMills(), config.getReadWriteRadio());
    }

    @Override
    public void recordBeginStatus(MysqlConnector mysqlConnector) {

    }

    @Override
    public void recordEndStatus(MysqlConnector mysqlConnector) throws SQLException {
        String sql = "select count(*) from phantom_read_record";
        errCount = mysqlConnector.getResult(sql);
    }

    @Override
    public boolean checkCorrect() {
        return errCount == 0;
    }
}
