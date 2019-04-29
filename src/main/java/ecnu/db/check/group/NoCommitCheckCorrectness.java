package ecnu.db.check.group;

import ecnu.db.check.BaseCheckCorrectness;
import ecnu.db.utils.MysqlConnector;

import java.sql.SQLException;

public class NoCommitCheckCorrectness extends BaseCheckCorrectness {
    private int errCount;

    public NoCommitCheckCorrectness() {
        super("SampleConfig.xml");
    }


    @Override
    public void makeTransaction() {

    }

    @Override
    public void recordBeginStatus(MysqlConnector mysqlConnector) throws SQLException {

    }

    @Override
    public void recordEndStatus(MysqlConnector mysqlConnector) throws SQLException {
        errCount = mysqlConnector.getNoCommitCount(workNodes.get(0).getTableIndex());
    }


    @Override
    public boolean checkCorrect() {
        return errCount == 0;
    }
}
