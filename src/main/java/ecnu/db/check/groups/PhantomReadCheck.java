package ecnu.db.check.groups;

import ecnu.db.check.BaseCheck;
import ecnu.db.transaction.PhantomRead;
import ecnu.db.utils.DatabaseConnector;

import java.sql.SQLException;

public class PhantomReadCheck extends BaseCheck {
    private int errCount;

    public PhantomReadCheck() {
        super("PhantomReadConfig.xml");
    }

    @Override
    public void makeTransaction() {
        transaction = new PhantomRead(checkNodes.get(0), config.getSleepMills(), config.getReadWriteRadio());
    }

    @Override
    public void recordBeginStatus(DatabaseConnector databaseConnector) {

    }

    @Override
    public void recordEndStatus(DatabaseConnector databaseConnector) throws SQLException {
        String sql = "select count(*) from phantom_read_record";
        errCount = databaseConnector.getResult(sql);
    }

    @Override
    public boolean checkCorrect() {
        return errCount == 0;
    }
}
