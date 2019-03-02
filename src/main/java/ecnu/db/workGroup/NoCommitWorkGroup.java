package ecnu.db.workGroup;

import ecnu.db.utils.MysqlConnector;

import java.sql.SQLException;

public class NoCommitWorkGroup extends BaseWorkGroup {
    private int errCount;

    NoCommitWorkGroup() {
        super(WorkGroupType.noCommit);
    }

    @Override
    public void computeAllSum(boolean isBegin, MysqlConnector mysqlConnector) throws SQLException {
        if(!isBegin){
            errCount=mysqlConnector.getNoCommitCount(in.get(0).getTableIndex());
        }
    }

    @Override
    public boolean checkCorrect() {
        return errCount==0;
    }
}
