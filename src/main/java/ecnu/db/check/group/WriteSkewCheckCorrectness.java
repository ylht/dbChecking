package ecnu.db.check.group;

import ecnu.db.check.BaseCheckCorrectness;
import ecnu.db.utils.MysqlConnector;

import java.sql.SQLException;

public class WriteSkewCheckCorrectness extends BaseCheckCorrectness {
    private int errCount;


    public WriteSkewCheckCorrectness() {
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
    public void recordBeginStatus(MysqlConnector mysqlConnector) throws SQLException { }

    @Override
    public void recordEndStatus(MysqlConnector mysqlConnector) throws SQLException {
        errCount = mysqlConnector.getWriteSkewResult(workNodes);
    }

    @Override
    public int getColumnCount() {
        return 0;
    }


    @Override
    public boolean checkCorrect() {
        return errCount == 0;
    }
}
