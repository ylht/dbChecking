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
        String sql = "select count(*) from t" + workNodes.get(0).getTableIndex() + " where checkRepeatableRead!=0";
        errCount = mysqlConnector.getResult(sql);
    }


    @Override
    public boolean checkCorrect() {
        return errCount == 0;
    }
}
