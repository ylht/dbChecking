package ecnu.db.threads.transaction;

import ecnu.db.checking.WorkGroup;
import ecnu.db.checking.WorkNode;
import ecnu.db.scheme.Table;
import ecnu.db.utils.MysqlConnector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

public class OrderBySelectTransaction implements Runnable {
    private static final Logger logger = LogManager.getLogger();
    private MysqlConnector mysqlConnector;
    private boolean addOrNot;
    private CountDownLatch count;
    private int runCount;
    private Table[] tables;
    private ArrayList<WorkNode> inNodes = new ArrayList<>();
    private ArrayList<WorkNode> outNodes = new ArrayList<>();
    private ArrayList<PreparedStatement> addSelectStatement = new ArrayList<>();
    private ArrayList<PreparedStatement> addStatement = new ArrayList<>();
    private ArrayList<PreparedStatement> subSelectStatement = new ArrayList<>();
    private ArrayList<PreparedStatement> subStatement = new ArrayList<>();

    public OrderBySelectTransaction(Table[] tables, WorkGroup workGroup, int runCount, CountDownLatch count, boolean forUpdate) {
        //确保工作组的类型正确
        assert workGroup.getWorkGroupType() == WorkGroup.WorkGroupType.order;
        mysqlConnector = new MysqlConnector();
        this.tables = tables;
        this.runCount = runCount;
        this.count = count;

        if (workGroup.getIn().size() != 0) {
            addOrNot = true;
            inNodes.addAll(workGroup.getIn());
            for (WorkNode inNode : inNodes) {
                addSelectStatement.add(mysqlConnector.getSelect(forUpdate, inNode.getTableIndex()
                        , inNode.getTupleIndex()));
                addStatement.add(mysqlConnector.getOrderUpdate(true, inNode.getTableIndex()
                        , inNode.getTupleIndex(), true));
            }
        } else {
            addOrNot = false;
            outNodes.addAll(workGroup.getOut());
            for (WorkNode outNode : outNodes) {
                subSelectStatement.add(mysqlConnector.getSelect(forUpdate, outNode.getTableIndex(),
                        outNode.getTupleIndex()));
                subStatement.add(mysqlConnector.getOrderUpdate(false, outNode.getTableIndex(),
                        outNode.getTupleIndex(), true));
            }
        }
    }

    @Override
    public void run() {
        Random r = new Random();
        Connection conn = mysqlConnector.getConn();
        WorkNode work;
        PreparedStatement preparedStatement;
        PreparedStatement preparedSelect;
        if (addOrNot) {
            int randomInIndex = r.nextInt(inNodes.size());
            work = inNodes.get(randomInIndex);
            preparedSelect = addSelectStatement.get(randomInIndex);
            preparedStatement = addStatement.get(randomInIndex);
        } else {
            int randomOutIndex = r.nextInt(outNodes.size());
            work = outNodes.get(randomOutIndex);
            preparedSelect = subSelectStatement.get(randomOutIndex);
            preparedStatement = subStatement.get(randomOutIndex);
        }
        for (int i = 0; i < runCount; i++) {
            try {
                int workPriKey = work.getSubValueList().get(
                        tables[work.getTableIndex()].getRandomKey() - 1);
                preparedSelect.setInt(1, workPriKey);
                ResultSet rs = preparedSelect.executeQuery();
                rs.next();
                preparedStatement.setDouble(1, rs.getDouble(1));
                preparedStatement.setInt(2, workPriKey);
                if (preparedStatement.executeUpdate() == 0) {
                    System.out.println("执行失败"+preparedStatement);
                    conn.rollback();
                    continue;
                }
                conn.commit();
                logger.trace(work.getTableIndex() + "," + work.getTupleIndex() + "," + workPriKey);
            } catch (SQLException e) {
                e.printStackTrace();
                System.out.println("执行失败"+preparedSelect);
                System.out.println("执行失败"+preparedStatement);
            }
        }
        count.countDown();
        mysqlConnector.close();
    }
}
