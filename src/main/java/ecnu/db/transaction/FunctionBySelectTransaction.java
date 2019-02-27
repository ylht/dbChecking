package ecnu.db.transaction;

import ecnu.db.checking.WorkGroup;
import ecnu.db.checking.WorkNode;
import ecnu.db.scheme.Table;
import ecnu.db.utils.MysqlConnector;

import java.sql.PreparedStatement;
import java.util.concurrent.CountDownLatch;

public class FunctionBySelectTransaction extends BaseTransaction {

    private int k;
    private Table[] tables;
    private WorkNode inNode;
    private WorkNode outNode;
    private PreparedStatement inSelectStatement;
    private PreparedStatement inStatement;
    private PreparedStatement outSelectStatement;
    private PreparedStatement outStatement;

    public FunctionBySelectTransaction(Table[] tables, WorkGroup workGroup,
                                       MysqlConnector mysqlConnector, boolean forUpdate) {
        super(mysqlConnector);
        //确保工作组的类型正确
        assert workGroup.getWorkGroupType() == WorkGroup.WorkGroupType.function;
        this.tables = tables;
        inNode = workGroup.getIn().get(0);
        outNode = workGroup.getOut().get(0);

        inSelectStatement = mysqlConnector.getSelect(forUpdate, inNode.getTableIndex(),
                inNode.getTupleIndex());

        inStatement = mysqlConnector.getRemittanceUpdate(true, inNode.getTableIndex(),
                inNode.getTupleIndex(), true);

        outSelectStatement = mysqlConnector.getSelect(forUpdate, outNode.getTableIndex(),
                outNode.getTupleIndex());

        outStatement = mysqlConnector.getRemittanceUpdate(true, outNode.getTableIndex(),
                outNode.getTupleIndex(), true);

        this.k = workGroup.getK();
    }

    @Override
    public void execute() {
        Double subNum = tables[outNode.getTableIndex()].getTransactionValue(outNode.getTupleIndex());
        RemittanceBySelectTransaction.workTransactionForSelect(conn, tables, subNum, k * subNum,
                outNode, outStatement, outSelectStatement,
                inNode, inStatement, inSelectStatement, true);
    }

}
