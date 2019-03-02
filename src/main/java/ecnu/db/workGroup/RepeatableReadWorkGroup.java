package ecnu.db.workGroup;

import ecnu.db.utils.MysqlConnector;

import java.sql.SQLException;

public class RepeatableReadWorkGroup extends BaseWorkGroup{
    private int errCount;
    RepeatableReadWorkGroup() {
        super(WorkGroupType.repeatableRead);
    }

    @Override
    public void computeAllSum(boolean isBegin, MysqlConnector mysqlConnector) throws SQLException {
        errCount=mysqlConnector.getSumRepeatableRead(in.get(0).getTableIndex());
    }

    @Override
    public boolean checkCorrect() {
        return errCount==0;
    }
}
