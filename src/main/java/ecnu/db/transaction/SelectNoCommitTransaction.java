package ecnu.db.transaction;

import ecnu.db.utils.MysqlConnector;
import ecnu.db.work.group.BaseWorkGroup;
import ecnu.db.work.group.WorkNode;

import java.sql.ResultSet;
import java.sql.SQLException;

public class SelectNoCommitTransaction extends BaseTransaction {
    private WorkNode node;

    public SelectNoCommitTransaction(BaseWorkGroup workGroup,
                                     MysqlConnector mysqlConnector) throws SQLException {
        super(mysqlConnector, false);
        node = workGroup.getIn().get(0);
        preparedOutSelectStatement = mysqlConnector.getSelect(false, node.getTableIndex(),
                node.getTupleIndex());
        preparedOutStatement=mysqlConnector.getInsertNoCommitColStatement(node.getTableIndex());
    }

    @Override
    public void execute() throws SQLException {
        int workInKey=node.getSubKey();
        preparedOutSelectStatement.setInt(1,workInKey);
        ResultSet rs=preparedOutSelectStatement.executeQuery();
        if(rs.next()){
            preparedOutStatement.setDouble(1,rs.getDouble(1));
            preparedOutStatement.setInt(2,workInKey);
            preparedOutStatement.executeUpdate();
        }
        mysqlConnector.commit();
    }
}
