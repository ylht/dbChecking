package ecnu.db.transaction;

import ecnu.db.core.WorkGroup;
import ecnu.db.core.WorkNode;
import ecnu.db.scheme.Table;
import ecnu.db.utils.MysqlConnector;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

/**
 * @author wangqingshuai
 */
public class RemittanceTransaction extends BaseTransaction {

    private Table[] tables;
    private ArrayList<WorkNode> inNode;
    private ArrayList<WorkNode> outNode;
    private ArrayList<PreparedStatement> addSelectStatement = new ArrayList<>();
    private ArrayList<PreparedStatement> addStatement = new ArrayList<>();
    private ArrayList<PreparedStatement> subSelectStatement = new ArrayList<>();
    private ArrayList<PreparedStatement> subStatement = new ArrayList<>();

    public RemittanceTransaction(Table[] tables, WorkGroup workGroup,
                                 MysqlConnector mysqlConnector, boolean forUpdate) throws SQLException {
        super(mysqlConnector, true);
        //确保工作组的类型正确
        remittanceTransaction(tables, workGroup, mysqlConnector);
        for (WorkNode node : inNode) {
            addSelectStatement.add(mysqlConnector.getSelect(forUpdate, node.getTableIndex(),
                    node.getTupleIndex()));
        }
        for (WorkNode node : outNode) {
            subSelectStatement.add(mysqlConnector.getSelect(forUpdate, node.getTableIndex(),
                    node.getTupleIndex()));
        }
    }

    public RemittanceTransaction(Table[] tables, WorkGroup workGroup,
                                 MysqlConnector mysqlConnector) throws SQLException {
        super(mysqlConnector, false);
        remittanceTransaction(tables, workGroup, mysqlConnector);
    }

    private void remittanceTransaction(Table[] tables, WorkGroup workGroup,
                                       MysqlConnector mysqlConnector) throws SQLException {
        //确保工作组的类型正确
        assert workGroup.getWorkGroupType() == WorkGroup.WorkGroupType.remittance;
        this.tables = tables;
        inNode = new ArrayList<>(workGroup.getIn());
        outNode = new ArrayList<>(workGroup.getOut());
        for (WorkNode node : inNode) {
            addStatement.add(mysqlConnector.getRemittanceUpdate(true,
                    node.getTableIndex(), node.getTupleIndex(), isSelect));
        }
        for (WorkNode node : outNode) {
            subStatement.add(mysqlConnector.getRemittanceUpdate(false,
                    node.getTableIndex(), node.getTupleIndex(), isSelect));
        }
    }


    @Override
    public void execute() throws SQLException {
        //随机做减的表格和SQL信息
        int randomOutIndex = r.nextInt(outNode.size());
        WorkNode workOut = outNode.get(randomOutIndex);
        int workOutPriKey = workOut.getSubValueList().get(tables[workOut.getTableIndex()].getDistributionIndex() - 1);

        Double subNum = tables[workOut.getTableIndex()].getTransactionValue(workOut.getTupleIndex());
        preparedOutStatement = subStatement.get(randomOutIndex);
        preparedOutSelectStatement = subSelectStatement.get(randomOutIndex);

        //随机做加的表格和SQL信息
        int randomInIndex = r.nextInt(inNode.size());
        WorkNode workIn = inNode.get(randomInIndex);
        int workInPriKey = workIn.getAddValueList().get(tables[workIn.getTableIndex()].getDistributionIndex() - 1);

        preparedInStatement = addStatement.get(randomInIndex);
        preparedInSelectStatement = addSelectStatement.get(randomInIndex);


        if (isSelect) {
            preparedOutSelectStatement.setInt(1, workOutPriKey);
            ResultSet rs = preparedOutSelectStatement.executeQuery();
            rs.next();
            preparedOutStatement.setDouble(1, rs.getDouble(1) - subNum);
        } else {
            preparedOutStatement.setDouble(1, subNum);
        }
        //设置主键
        preparedOutStatement.setInt(2, workOutPriKey);
        preparedOutStatement.setDouble(3, subNum);

        if (preparedOutStatement.executeUpdate() == 0) {
            System.out.println("执行失败:" + preparedOutStatement);
            mysqlConnector.rollback();
            return;
        }

        if (!FunctionTransaction.executeAdd(isSelect, preparedInSelectStatement,
                preparedInStatement, workInPriKey, subNum)) {
            mysqlConnector.rollback();
            return;
        }

        mysqlConnector.commit();


    }
}
