package ecnu.db.transaction;

import ecnu.db.core.WorkGroup;
import ecnu.db.core.WorkNode;
import ecnu.db.scheme.Table;
import ecnu.db.utils.MysqlConnector;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author wangqingshuai
 * 处理fuction类型事务的线程
 */
public class FunctionTransaction extends BaseTransaction {

    private int k;
    private Table[] tables;
    private WorkNode inNode;
    private WorkNode outNode;


    public FunctionTransaction(Table[] tables, WorkGroup workGroup,
                               MysqlConnector mysqlConnector) throws SQLException {
        super(mysqlConnector, false);
        functionTransaction(tables, workGroup, mysqlConnector);
    }

    public FunctionTransaction(Table[] tables, WorkGroup workGroup,
                               MysqlConnector mysqlConnector, boolean forUpdate) throws SQLException {
        super(mysqlConnector, true);
        functionTransaction(tables, workGroup, mysqlConnector);
        preparedInSelectStatement = mysqlConnector.getSelect(forUpdate,
                inNode.getTableIndex(), inNode.getTupleIndex());
        preparedOutSelectStatement = mysqlConnector.getSelect(forUpdate,
                outNode.getTableIndex(), outNode.getTupleIndex());
    }

    static boolean executeAdd(boolean isSelect, PreparedStatement preparedSelectStatement,
                              PreparedStatement preparedStatement,
                              int workPriKey, double subNum) throws SQLException {
        if (isSelect) {
            preparedSelectStatement.setInt(1, workPriKey);
            ResultSet rs = preparedSelectStatement.executeQuery();
            rs.next();
            preparedStatement.setDouble(1, rs.getDouble(1) + subNum);
        } else {
            preparedStatement.setDouble(1, subNum);
        }

        //设置主键
        preparedStatement.setInt(2, workPriKey);

        if (preparedStatement.executeUpdate() == 0) {
            System.out.println("执行失败:" + preparedStatement);
            return false;
        }
        return true;
    }

    private void functionTransaction(Table[] tables,
                                     WorkGroup workGroup, MysqlConnector mysqlConnector) throws SQLException {
        //确保工作组的类型正确
        assert workGroup.getWorkGroupType() == WorkGroup.WorkGroupType.function;
        this.tables = tables;
        inNode = workGroup.getIn().get(0);
        outNode = workGroup.getOut().get(0);
        preparedInStatement = mysqlConnector.getRemittanceUpdate(true, inNode.getTableIndex(),
                inNode.getTupleIndex(), isSelect);

        preparedOutStatement = mysqlConnector.getRemittanceUpdate(true, outNode.getTableIndex(),
                outNode.getTupleIndex(), isSelect);

        this.k = workGroup.getK();
    }

    @Override
    public void execute() throws SQLException {
        Double subNum = tables[outNode.getTableIndex()].getTransactionValue(outNode.getTupleIndex());
        int workOutPriKey = outNode.getSubValueList().get(tables[outNode.getTableIndex()].getDistributionIndex() - 1);
        int workInPriKey = inNode.getAddValueList().get(tables[inNode.getTableIndex()].getDistributionIndex() - 1);

        if (!executeAdd(isSelect, preparedOutSelectStatement, preparedOutStatement, workOutPriKey, subNum)) {
            mysqlConnector.rollback();
            return;
        }
        if (executeAdd(isSelect, preparedInSelectStatement, preparedInStatement, workInPriKey, subNum * k)) {
            mysqlConnector.rollback();
            return;
        }
        mysqlConnector.rollback();

    }
}
