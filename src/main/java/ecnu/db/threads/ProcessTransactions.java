package ecnu.db.threads;

import ecnu.db.checking.WorkGroup;
import ecnu.db.checking.WorkNode;
import ecnu.db.scheme.Table;
import ecnu.db.utils.MysqlConnector;
import org.apache.commons.math3.distribution.ZipfDistribution;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;

/**
 * @author wangqingshuai
 */
public class ProcessTransactions implements Runnable {
    private MysqlConnector mysqlConnector;
    private CountDownLatch count;
    private int runCount;
    private Table[] tables;
    private ArrayList<WorkNode> inNode;
    private ArrayList<WorkNode> outNode;
    private ArrayList<PreparedStatement> addStatement = new ArrayList<>();
    private ArrayList<PreparedStatement> subStatement = new ArrayList<>();

    public ProcessTransactions(Table[] tables, WorkGroup workGroup, int runCount,CountDownLatch count) {
        mysqlConnector = new MysqlConnector();
        this.tables = tables;
        inNode = new ArrayList<>(workGroup.getIn());
        outNode = new ArrayList<>(workGroup.getOut());
        for (WorkNode node : inNode) {
            addStatement.add(mysqlConnector.getPrepareUpdate(true, node.getTableIndex()
                    , node.getTupleIndex()));
        }
        for (WorkNode node : outNode) {
            subStatement.add(mysqlConnector.getPrepareUpdate(false, node.getTableIndex(),
                    node.getTupleIndex()));
        }
        this.runCount = runCount;
        this.count=count;
    }

    @Override
    public void run() {
        Random r = new Random();
        ZipfDistribution zf = new ZipfDistribution(1000, 1);
        Connection conn = mysqlConnector.getConn();
        try {
            conn.setAutoCommit(false);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        for (int i = 0; i < runCount; i++) {
            int randomOutIndex = r.nextInt(outNode.size());
            WorkNode workOut = outNode.get(randomOutIndex);
            Double subNum = tables[workOut.getTableIndex()].getTransactionValue(workOut.getTupleIndex());
            PreparedStatement preparedOutStatement = subStatement.get(randomOutIndex);
            int randomInIndex = r.nextInt(inNode.size());
            WorkNode workIn = inNode.get(randomInIndex);
            PreparedStatement preparedInStatement = addStatement.get(randomInIndex);
            try {
                preparedOutStatement.setDouble(1, subNum);
                int workOutPriKey = workOut.getSubValueList().get(zf.sample() - 1);
                preparedOutStatement.setInt(2, workOutPriKey);
                preparedOutStatement.setDouble(3, subNum);
                if (preparedOutStatement.executeUpdate() == 0) {
                    System.out.println(preparedOutStatement.toString());
                    conn.rollback();
                    continue;
                }
                preparedInStatement.setDouble(1, subNum);
                int workInPriKey = workIn.getAddValueList().get(zf.sample() - 1);
                preparedInStatement.setInt(2, workInPriKey);
                preparedInStatement.setDouble(3, tables[workIn.getTableIndex()].
                        getMaxValue(workIn.getTupleIndex()) - subNum);
                if (preparedInStatement.executeUpdate() == 0) {
                    System.out.println(preparedInStatement.toString());
                    conn.rollback();
                    continue;
                }
                conn.commit();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        count.countDown();
        mysqlConnector.close();
    }
}
