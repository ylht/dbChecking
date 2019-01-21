package ecnu.db.threads;

import ecnu.db.checking.WorkGroup;
import ecnu.db.checking.WorkNode;
import ecnu.db.scheme.Table;
import ecnu.db.utils.MysqlConnector;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

/**
 * @author wangqingshuai
 */
public class RemittanceTransaction implements Runnable {
    private MysqlConnector mysqlConnector;
    private CountDownLatch count;
    private int runCount;
    private Table[] tables;
    private ArrayList<WorkNode> inNode;
    private ArrayList<WorkNode> outNode;
    private ArrayList<PreparedStatement> addStatement = new ArrayList<>();
    private ArrayList<PreparedStatement> subStatement = new ArrayList<>();

    public RemittanceTransaction(Table[] tables, WorkGroup workGroup, int runCount, CountDownLatch count) {
        //确保工作组的类型正确
        assert workGroup.getWorkGroupType() == WorkGroup.WorkGroupType.remittance;
        mysqlConnector = new MysqlConnector();
        this.tables = tables;
        inNode = new ArrayList<>(workGroup.getIn());
        outNode = new ArrayList<>(workGroup.getOut());
        for (WorkNode node : inNode) {
            addStatement.add(mysqlConnector.getRemittanceUpdate(true, node.getTableIndex()
                    , node.getTupleIndex()));
        }
        for (WorkNode node : outNode) {
            subStatement.add(mysqlConnector.getRemittanceUpdate(false, node.getTableIndex(),
                    node.getTupleIndex()));
        }
        this.runCount = runCount;
        this.count = count;
    }

    static void workForTransaction(Connection conn, WorkNode workOut,
                                   Double subNum,
                                   PreparedStatement preparedOutStatement,
                                   WorkNode workIn, PreparedStatement preparedInStatement,
                                   Table[] tables, Double subNum2,boolean isFunction) throws SQLException {
        preparedOutStatement.setDouble(1, subNum);
        int workOutPriKey = workOut.getSubValueList().get(
                tables[workOut.getTableIndex()].getRandomKey() - 1);
        preparedOutStatement.setInt(2, workOutPriKey);
        if(isFunction){
            preparedOutStatement.setDouble(3,tables[workOut.getTableIndex()].
                    getMaxValue(workOut.getTupleIndex()) - subNum);
        }else {
            preparedOutStatement.setDouble(3, subNum);
        }
        if (preparedOutStatement.executeUpdate() == 0) {
            System.out.println(preparedOutStatement.toString());
            conn.rollback();
            return;
        }
        preparedInStatement.setDouble(1, subNum2);
        int workInPriKey = workIn.getAddValueList().get(
                tables[workIn.getTableIndex()].getRandomKey() - 1);
        preparedInStatement.setInt(2, workInPriKey);
        preparedInStatement.setDouble(3, tables[workIn.getTableIndex()].
                getMaxValue(workIn.getTupleIndex()) - subNum);
        if (preparedInStatement.executeUpdate() == 0) {
            System.out.println(preparedInStatement.toString());
            conn.rollback();
            return;
        }
        conn.commit();
    }

    @Override
    public void run() {
        Random r = new Random();
        Connection conn = mysqlConnector.getConn();

        for (int i = 0; i < runCount; i++) {
            int randomOutIndex = r.nextInt(outNode.size());
            WorkNode workOut = outNode.get(randomOutIndex);
            Double subNum = tables[workOut.getTableIndex()].getTransactionValue(workOut.getTupleIndex());
            PreparedStatement preparedOutStatement = subStatement.get(randomOutIndex);
            int randomInIndex = r.nextInt(inNode.size());
            WorkNode workIn = inNode.get(randomInIndex);
            PreparedStatement preparedInStatement = addStatement.get(randomInIndex);
            try {
                workForTransaction(conn, workOut, subNum, preparedOutStatement,
                        workIn, preparedInStatement, tables, subNum,false);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        count.countDown();
        mysqlConnector.close();
    }
}
