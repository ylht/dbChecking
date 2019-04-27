package ecnu.db.check.group;

import ecnu.db.check.BaseCheckCorrectness;
import ecnu.db.utils.MysqlConnector;

import java.sql.SQLException;

public class NoCommitCheckCorrectness extends BaseCheckCorrectness {
    private final static int COLUMN_COUNT = 2;
    private int errCount;

    public NoCommitCheckCorrectness() {
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
    public int getColumnCount() {
        return COLUMN_COUNT;
    }

    @Override
    public void computeAllSum(boolean isBegin, MysqlConnector mysqlConnector) throws SQLException {
        if (!isBegin) {
            errCount = mysqlConnector.getNoCommitCount(workNodes.get(0).getTableIndex());
        }
    }

    @Override
    public boolean checkCorrect() {
        return errCount == 0;
    }
}
