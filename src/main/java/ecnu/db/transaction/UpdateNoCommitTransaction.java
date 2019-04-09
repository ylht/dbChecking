package ecnu.db.transaction;

import ecnu.db.utils.MysqlConnector;
import ecnu.db.work.group.BaseWorkGroup;
import ecnu.db.work.group.WorkNode;

import java.sql.SQLException;

public class UpdateNoCommitTransaction extends BaseTransaction {
    private WorkNode node;

    public UpdateNoCommitTransaction(BaseWorkGroup workGroup,
                                     MysqlConnector mysqlConnector) throws SQLException {
        super(mysqlConnector, false);
        //确保工作组的类型正确
        assert workGroup.getWorkGroupType() == BaseWorkGroup.WorkGroupType.noCommit;
        node = workGroup.getIn().get(0);
        preparedOutStatement = mysqlConnector.getUpdateNoCommitStatement(node.getTableIndex()
                , node.getTupleIndex());
    }

    @Override
    public void execute() throws SQLException {
        if (r.nextDouble() < 0.99) {
            return;
        }
        int min = node.getSubKey();
        int max = node.getSubKey();
        if (min > max) {
            int temp = max;
            max = min;
            min = temp;
        }
        preparedOutStatement.setInt(1, min);
        preparedOutStatement.setInt(2, max);
        preparedOutStatement.executeUpdate();
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        mysqlConnector.rollback();
    }
}
