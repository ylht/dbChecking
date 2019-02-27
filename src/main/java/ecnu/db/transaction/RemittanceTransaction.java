package ecnu.db.transaction;

import ecnu.db.checking.WorkGroup;
import ecnu.db.checking.WorkNode;
import ecnu.db.scheme.Table;
import ecnu.db.utils.MysqlConnector;
import org.apache.logging.log4j.LogManager;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;

/**
 * @author wangqingshuai
 */
public class RemittanceTransaction extends BaseTransaction {

    private Table[] tables;
    private ArrayList<WorkNode> inNode;
    private ArrayList<WorkNode> outNode;
    private ArrayList<PreparedStatement> addStatement = new ArrayList<>();
    private ArrayList<PreparedStatement> subStatement = new ArrayList<>();

    public RemittanceTransaction(Table[] tables, WorkGroup workGroup, MysqlConnector mysqlConnector) {
        super(mysqlConnector);
        //确保工作组的类型正确
        assert workGroup.getWorkGroupType() == WorkGroup.WorkGroupType.remittance;
        this.tables = tables;
        inNode = new ArrayList<>(workGroup.getIn());
        outNode = new ArrayList<>(workGroup.getOut());
        for (WorkNode node : inNode) {
            addStatement.add(mysqlConnector.getRemittanceUpdate(true,
                    node.getTableIndex(), node.getTupleIndex(), false));
        }
        for (WorkNode node : outNode) {
            subStatement.add(mysqlConnector.getRemittanceUpdate(false,
                    node.getTableIndex(), node.getTupleIndex(), false));
        }
    }

    static void workForTransaction(boolean isFunction, Table[] tables, Connection conn,
                                   PreparedStatement preparedOutStatement,
                                   WorkNode workOut, Double subNum,
                                   PreparedStatement preparedInStatement,
                                   WorkNode workIn, Double addNum) throws SQLException {
        preparedOutStatement.setDouble(1, subNum);
        int workOutPriKey = workOut.getSubValueList().get(
                tables[workOut.getTableIndex()].getRandomKey() - 1);
        preparedOutStatement.setInt(2, workOutPriKey);
        if (isFunction) {
            preparedOutStatement.setDouble(3, tables[workOut.getTableIndex()].
                    getMaxValue(workOut.getTupleIndex()) - subNum);
        } else {
            preparedOutStatement.setDouble(3, subNum);
        }
        if (preparedOutStatement.executeUpdate() == 0) {
            System.out.println("执行失败：" + preparedOutStatement);
            conn.rollback();
            return;
        }
        preparedInStatement.setDouble(1, addNum);
        int workInPriKey = workIn.getAddValueList().get(
                tables[workIn.getTableIndex()].getRandomKey() - 1);
        preparedInStatement.setInt(2, workInPriKey);
        preparedInStatement.setDouble(3, tables[workIn.getTableIndex()].
                getMaxValue(workIn.getTupleIndex()) - subNum);
        if (preparedInStatement.executeUpdate() == 0) {
            System.out.println("执行失败" + preparedInStatement);
            conn.rollback();
            return;
        }
        conn.commit();
    }

    @Override
    public void execute() {
        int randomOutIndex = r.nextInt(outNode.size());
        WorkNode workOut = outNode.get(randomOutIndex);
        Double subNum = tables[workOut.getTableIndex()].getTransactionValue(workOut.getTupleIndex());
        PreparedStatement preparedOutStatement = subStatement.get(randomOutIndex);
        int randomInIndex = r.nextInt(inNode.size());
        WorkNode workIn = inNode.get(randomInIndex);
        PreparedStatement preparedInStatement = addStatement.get(randomInIndex);

        try {
            workForTransaction(false, tables, conn,
                    preparedOutStatement, workOut, subNum,
                    preparedInStatement, workIn, subNum);
        } catch (SQLException e) {
            LogManager.getLogger().error(e);
        }
    }

}

