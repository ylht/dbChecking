package ecnu.db.workGroup;

import ecnu.db.utils.MysqlConnector;

import java.sql.SQLException;

public class PhantomReadWorkGroup extends BaseWorkGroup {
    private boolean changeTableSize;
    private int errCount;
    public boolean isChangeTableSize() {
        return changeTableSize;
    }

    PhantomReadWorkGroup(boolean changeTableSize) {
        super(WorkGroupType.phantomRead);
        this.changeTableSize=changeTableSize;
    }

    @Override
    public void computeAllSum(boolean isBegin, MysqlConnector mysqlConnector) throws SQLException {
        errCount=mysqlConnector.getPhantomRecordNum();
    }

    @Override
    public boolean checkCorrect() {
        return errCount==0;
    }
}
