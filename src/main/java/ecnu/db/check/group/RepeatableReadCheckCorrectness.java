package ecnu.db.check.group;

import ecnu.db.check.BaseCheckCorrectness;
import ecnu.db.utils.MysqlConnector;

import java.sql.SQLException;

public class RepeatableReadCheckCorrectness extends BaseCheckCorrectness {
    private int errCount;

    public RepeatableReadCheckCorrectness() {
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
        errCount = mysqlConnector.getSumRepeatableRead(workNodes.get(0).getTableIndex());
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