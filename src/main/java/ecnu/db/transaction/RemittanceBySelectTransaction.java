package ecnu.db.transaction;

import ecnu.db.checking.WorkGroup;
import ecnu.db.checking.WorkNode;
import ecnu.db.scheme.Table;
import ecnu.db.utils.MysqlConnector;
import org.apache.logging.log4j.LogManager;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

/**
 * @author wangqingshuai
 */
public class RemittanceBySelectTransaction implements Runnable {
    private MysqlConnector mysqlConnector;
    private CountDownLatch count;
    private int runCount;
    private Table[] tables;
    private ArrayList<WorkNode> inNode;
    private ArrayList<WorkNode> outNode;
    private ArrayList<PreparedStatement> addSelectStatement = new ArrayList<>();
    private ArrayList<PreparedStatement> addStatement = new ArrayList<>();
    private ArrayList<PreparedStatement> subSelectStatement = new ArrayList<>();
    private ArrayList<PreparedStatement> subStatement = new ArrayList<>();

    public RemittanceBySelectTransaction(Table[] tables, WorkGroup workGroup, int runCount, CountDownLatch count, boolean forUpdate) {
        //确保工作组的类型正确
        assert workGroup.getWorkGroupType() == WorkGroup.WorkGroupType.remittance;
        mysqlConnector = new MysqlConnector();
        this.tables = tables;
        inNode = new ArrayList<>(workGroup.getIn());
        outNode = new ArrayList<>(workGroup.getOut());
        for (WorkNode node : inNode) {
            addSelectStatement.add(mysqlConnector.getSelect(forUpdate, node.getTableIndex(),
                    node.getTupleIndex()));
            addStatement.add(mysqlConnector.getRemittanceUpdate(true, node.getTableIndex(),
                    node.getTupleIndex(), true));
        }
        for (WorkNode node : outNode) {
            subSelectStatement.add(mysqlConnector.getSelect(forUpdate, node.getTableIndex(),
                    node.getTupleIndex()));
            subStatement.add(mysqlConnector.getRemittanceUpdate(false, node.getTableIndex(),
                    node.getTupleIndex(), true));
        }
        this.runCount = runCount;
        this.count = count;
    }

    static boolean workTransactionForSelect(Connection conn, Table[] tables, Double subNum, Double subNum2,
                                            WorkNode workOut, PreparedStatement preparedOutStatement,
                                            PreparedStatement preparedOutSelectStatement,
                                            WorkNode workIn, PreparedStatement preparedInStatement,
                                            PreparedStatement preparedInSelectStatement, Boolean isFunction) {
        try {
            int workOutPriKey = workOut.getSubValueList().get(
                    tables[workOut.getTableIndex()].getRandomKey() - 1);
            preparedOutSelectStatement.setInt(1, workOutPriKey);
            ResultSet rs = preparedOutSelectStatement.executeQuery();
            rs.next();
            preparedOutStatement.setDouble(1, rs.getDouble(1));
            preparedOutStatement.setDouble(2, subNum);
            preparedOutStatement.setInt(3, workOutPriKey);
            if (isFunction) {
                preparedOutStatement.setDouble(4, tables[workOut.getTableIndex()].
                        getMaxValue(workOut.getTupleIndex()) - subNum);
            } else {
                preparedOutStatement.setDouble(4, subNum);
            }

            if (preparedOutStatement.executeUpdate() == 0) {
                System.out.println("执行失败" + preparedOutStatement);
                conn.rollback();
                return true;
            }

            int workInPriKey = workIn.getAddValueList().get(
                    tables[workIn.getTableIndex()].getRandomKey() - 1);
            preparedInSelectStatement.setInt(1, workInPriKey);
            rs = preparedInSelectStatement.executeQuery();
            rs.next();
            preparedInStatement.setDouble(1, rs.getDouble(1));
            preparedInStatement.setDouble(2, subNum2);
            preparedInStatement.setInt(3, workInPriKey);
            preparedInStatement.setDouble(4, tables[workIn.getTableIndex()].
                    getMaxValue(workIn.getTupleIndex()) - subNum2);
            if (preparedInStatement.executeUpdate() == 0) {
                System.out.println("执行失败" + preparedInStatement);
                conn.rollback();
                return true;
            }
            conn.commit();
        } catch (SQLException e) {
            LogManager.getLogger().error(e);
        }
        return false;
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
            PreparedStatement preparedOutSelectStatement = subSelectStatement.get(randomOutIndex);
            int randomInIndex = r.nextInt(inNode.size());
            WorkNode workIn = inNode.get(randomInIndex);
            PreparedStatement preparedInStatement = addStatement.get(randomInIndex);
            PreparedStatement preparedInSelectStatement = addSelectStatement.get(randomInIndex);
            if (workTransactionForSelect(conn, tables, subNum, subNum, workOut, preparedOutStatement, preparedOutSelectStatement,
                    workIn, preparedInStatement, preparedInSelectStatement, false)) {
                break;
            }
        }
        count.countDown();
        mysqlConnector.close();
    }
}
