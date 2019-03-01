package ecnu.db.transaction;

import ecnu.db.core.WorkGroup;
import ecnu.db.core.WorkNode;
import ecnu.db.scheme.Table;
import ecnu.db.utils.MysqlConnector;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

public class WriteSkewTransaction extends BaseTransaction {
    private Table[] tables;
    private ArrayList<WorkNode> nodes;
    private ArrayList<PreparedStatement> preparedSelectStatements = new ArrayList<>();
    private ArrayList<PreparedStatement> preparedStatements = new ArrayList<>();

    public WriteSkewTransaction(Table[] tables, WorkGroup workGroup,
                                MysqlConnector mysqlConnector) throws SQLException {
        super(mysqlConnector, false);
        assert workGroup.getWorkGroupType() == WorkGroup.WorkGroupType.writeSkew;

        this.tables = tables;
        nodes = workGroup.getOut();
        for (WorkNode node : nodes) {
            preparedSelectStatements.add(mysqlConnector.getSelect(false,
                    node.getTableIndex(), node.getTupleIndex()));
            preparedStatements.add(mysqlConnector.getWriteSkewUpdate(
                    node.getTableIndex(), node.getTupleIndex()));

        }
    }

    @Override
    public void execute() throws SQLException {
        int randomOutIndex = r.nextInt(nodes.size());
        preparedOutStatement = preparedStatements.get(randomOutIndex);
        preparedOutSelectStatement = preparedSelectStatements.get(randomOutIndex);
        WorkNode workOut = nodes.get(randomOutIndex);
        int randomInIndex;
        do {
            randomInIndex = r.nextInt(nodes.size());
        } while (randomInIndex == randomOutIndex);

        preparedInSelectStatement = preparedSelectStatements.get(randomInIndex);

        int workOutPriKey = workOut.getSubValueList().get(
                tables[workOut.getTableIndex()].getDistributionIndex() - 1);

        preparedOutSelectStatement.setInt(1, workOutPriKey);
        ResultSet rsOut = preparedOutSelectStatement.executeQuery();
        if (rsOut.next()) {
            double out = rsOut.getDouble(1);
            preparedInSelectStatement.setInt(1, workOutPriKey);
            ResultSet rsIn = preparedInSelectStatement.executeQuery();
            if (rsIn.next()) {
                double total = rsIn.getDouble(1) + out;
                if (total > 0) {
                    preparedOutStatement.setObject(1, (total) * 4 / 5);
                    preparedOutStatement.setInt(2, workOutPriKey);
                    if (preparedOutStatement.executeUpdate() == 0) {
                        System.out.println("执行失败" + preparedOutStatement.toString());
                        mysqlConnector.rollback();
                        return;
                    }
                    mysqlConnector.commit();
                    return;
                }
            }
        }
        mysqlConnector.commit();

    }
}
