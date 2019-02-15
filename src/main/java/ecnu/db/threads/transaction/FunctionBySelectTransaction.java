package ecnu.db.threads.transaction;

import ecnu.db.checking.WorkGroup;
import ecnu.db.checking.WorkNode;
import ecnu.db.scheme.Table;
import ecnu.db.utils.MysqlConnector;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.concurrent.CountDownLatch;

public class FunctionBySelectTransaction implements Runnable {
    private MysqlConnector mysqlConnector;
    private CountDownLatch count;
    private int k;
    private int runCount;
    private Table[] tables;
    private WorkNode inNode;
    private WorkNode outNode;
    private PreparedStatement inSelectStatement;
    private PreparedStatement inStatement;
    private PreparedStatement outSelectStatement;
    private PreparedStatement outStatement;

    public FunctionBySelectTransaction(Table[] tables, WorkGroup workGroup, int runCount, CountDownLatch count, boolean forUpdate) {
        //确保工作组的类型正确
        assert workGroup.getWorkGroupType() == WorkGroup.WorkGroupType.function;
        mysqlConnector = new MysqlConnector();
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

        this.runCount = runCount;
        this.count = count;
        this.k = workGroup.getK();
    }

    @Override
    public void run() {
        Connection conn = mysqlConnector.getConn();

        for (int i = 0; i < runCount; i++) {
            Double subNum = tables[outNode.getTableIndex()].getTransactionValue(outNode.getTupleIndex());
            if (RemittanceBySelectTransaction.workTransactionForSelect(conn, tables, subNum, k * subNum,
                    outNode, outStatement, outSelectStatement,
                    inNode, inStatement, inSelectStatement, true)) {
                break;
            }
        }
        count.countDown();
        mysqlConnector.close();
    }

}
