package ecnu.db.check.groups;

import ecnu.db.check.BaseCheck;
import ecnu.db.transaction.ReadCommitted;
import ecnu.db.utils.DatabaseConnector;

import java.sql.SQLException;

public class ReadCommittedCheck extends BaseCheck {
    private int errCount;

    public ReadCommittedCheck() {
        super("ReadCommittedConfig.xml");
    }


    @Override
    public void makeTransaction() {
        transaction = new ReadCommitted(checkNodes.get(0), config.getSleepMills(), config.getReadWriteRadio());
    }

    @Override
    public void recordBeginStatus(DatabaseConnector databaseConnector) {

    }

    @Override
    public void recordEndStatus(DatabaseConnector databaseConnector) throws SQLException {
        String sql = "select count(*) from t" + checkNodes.get(0).getTableIndex() + " where checkReadCommitted<0";
        errCount = databaseConnector.getResult(sql);
    }


    @Override
    public boolean checkCorrect() {
        return errCount == 0;
    }
}
