package ecnu.db.work.group;

import ecnu.db.utils.MysqlConnector;

import java.sql.SQLException;

class WriteSkewWorkGroup extends BaseWorkGroup {
    private int errCount;
    WriteSkewWorkGroup() {
        super(WorkGroupType.writeSkew);
    }

    @Override
    public void computeAllSum(boolean isBegin, MysqlConnector mysqlConnector) throws SQLException {
        if(!isBegin){
            errCount = mysqlConnector.getWriteSkewResult(out);
        }
    }

    @Override
    public boolean checkCorrect() {
        return errCount == 0;
    }
}
