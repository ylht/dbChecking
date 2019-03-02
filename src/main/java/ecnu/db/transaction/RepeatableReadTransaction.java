package ecnu.db.transaction;

import ecnu.db.utils.MysqlConnector;
import ecnu.db.workGroup.BaseWorkGroup;
import ecnu.db.workGroup.WorkNode;

import java.sql.ResultSet;
import java.sql.SQLException;

public class RepeatableReadTransaction extends BaseTransaction {
    private WorkNode node;
    public RepeatableReadTransaction(BaseWorkGroup workGroup,
                                     MysqlConnector mysqlConnector) throws SQLException {
        super(mysqlConnector, false);
        node = workGroup.getIn().get(0);
        preparedOutSelectStatement = mysqlConnector.getSelect(false, node.getTableIndex(),
                node.getTupleIndex());
        preparedOutStatement=mysqlConnector.getInsertRepeatableReadColStatement(node.getTableIndex());

    }

    @Override
    public void execute() throws SQLException {
        if(r.nextDouble()<0.8){
            return;
        }
        int workInKey=node.getSubKey();
        preparedOutSelectStatement.setInt(1,workInKey);
        ResultSet rs=preparedOutSelectStatement.executeQuery();
        if(rs.next()){
            double oldValue=rs.getDouble(1);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            rs=preparedOutSelectStatement.executeQuery();
            if(rs.next()){
                preparedOutStatement.setDouble(1,rs.getDouble(1)-oldValue);
                preparedOutStatement.setInt(2,workInKey);
                preparedOutStatement.executeUpdate();
            }
        }
        mysqlConnector.commit();
    }
}
