package ecnu.db.check.group;

import ecnu.db.check.BaseCheckCorrectness;
import ecnu.db.utils.MysqlConnector;

import java.sql.SQLException;

public class PhantomReadCheckCorrectness extends BaseCheckCorrectness {
    private int errCount;

    public PhantomReadCheckCorrectness() {
        super("SampleConfig.xml");
    }

    @Override
    public boolean workOnWorked() {
        return false;
    }


    @Override
    public void makeTransaction() {

    }

    @Override
    public void recordBeginStatus(MysqlConnector mysqlConnector) throws SQLException {

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
