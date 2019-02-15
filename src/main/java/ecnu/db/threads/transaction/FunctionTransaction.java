package ecnu.db.threads.transaction;

import ecnu.db.checking.WorkGroup;
import ecnu.db.checking.WorkNode;
import ecnu.db.scheme.Table;
import ecnu.db.utils.MysqlConnector;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.CountDownLatch;

/**
 * @author wangqingshuai
 * 处理fuction类型事务的线程
 */
public class FunctionTransaction implements Runnable {
    private MysqlConnector mysqlConnector;
    private CountDownLatch count;
    private int k;
    private int runCount;
    private Table[] tables;
    private WorkNode inNode;
    private WorkNode outNode;
    private PreparedStatement addInStatement;
    private PreparedStatement addOutStatement;

    public FunctionTransaction(Table[] tables, WorkGroup workGroup, int runCount, CountDownLatch count) {
        //确保工作组的类型正确
        assert workGroup.getWorkGroupType() == WorkGroup.WorkGroupType.function;
        mysqlConnector = new MysqlConnector();
        this.tables = tables;
        inNode = workGroup.getIn().get(0);
        outNode = workGroup.getOut().get(0);

        addInStatement = mysqlConnector.getRemittanceUpdate(true, inNode.getTableIndex()
                , inNode.getTupleIndex(), false);

        addOutStatement = mysqlConnector.getRemittanceUpdate(true, outNode.getTableIndex(),
                outNode.getTupleIndex(), false);

        this.runCount = runCount;
        this.count = count;
        this.k = workGroup.getK();
    }

    @Override
    public void run() {
        Connection conn = mysqlConnector.getConn();

        for (int i = 0; i < runCount; i++) {
            Double subNum = tables[outNode.getTableIndex()].getTransactionValue(outNode.getTupleIndex());
            try {
                RemittanceTransaction.workForTransaction(conn, outNode, subNum, addOutStatement,
                        inNode, addInStatement, tables, subNum * k, true);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        count.countDown();
        mysqlConnector.close();
    }

}
