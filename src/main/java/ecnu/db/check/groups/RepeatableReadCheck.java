package ecnu.db.check.groups;

import ecnu.db.check.BaseCheck;
import ecnu.db.transaction.RepeatableRead;
import ecnu.db.utils.DatabaseConnector;

import java.sql.SQLException;

public class RepeatableReadCheck extends BaseCheck {
    private int errCount;

    public RepeatableReadCheck() {
        super("RepeatableReadConfig.xml");
    }

    @Override
    public void makeTransaction() {
        transaction = new RepeatableRead(checkNodes.get(0), config.getSleepMills(), config.getReadWriteRadio());
    }

    @Override
    public void recordBeginStatus(DatabaseConnector databaseConnector) {

    }

    @Override
    public void recordEndStatus(DatabaseConnector databaseConnector) throws SQLException {
        String sql = "select count(*) from t" + checkNodes.get(0).getTableIndex() + " where checkRepeatableRead!=0";
        errCount = databaseConnector.getResult(sql);
    }


    @Override
    public boolean checkCorrect() {
        return errCount == 0;
    }
}
