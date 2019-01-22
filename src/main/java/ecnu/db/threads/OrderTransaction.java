package ecnu.db.threads;

import ecnu.db.checking.WorkGroup;
import ecnu.db.checking.WorkNode;
import ecnu.db.scheme.Table;
import ecnu.db.utils.MysqlConnector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

/**
 * @author wangqingshuai
 * 处理order类型事务的线程
 */
public class OrderTransaction implements Runnable {

    private static final Logger logger = LogManager.getLogger();
    private MysqlConnector mysqlConnector;
    private boolean addOrNot;
    private CountDownLatch count;
    private int runCount;
    private Table[] tables;
    private ArrayList<WorkNode> inNodes = new ArrayList<>();
    private ArrayList<WorkNode> outNodes = new ArrayList<>();
    private ArrayList<PreparedStatement> addStatement = new ArrayList<>();
    private ArrayList<PreparedStatement> subStatement = new ArrayList<>();

    public OrderTransaction(Table[] tables, WorkGroup workGroup, int runCount, CountDownLatch count) {
        //确保工作组的类型正确
        assert workGroup.getWorkGroupType() == WorkGroup.WorkGroupType.order;
        mysqlConnector = new MysqlConnector();
        this.tables = tables;
        this.runCount = runCount;
        this.count = count;

        if (workGroup.getIn() != null) {
            addOrNot = true;
            inNodes.addAll(workGroup.getIn());
            for (WorkNode inNode : inNodes) {
                addStatement.add(mysqlConnector.getOrderUpdate(true, inNode.getTableIndex()
                        , inNode.getTupleIndex()));
            }
        } else {
            addOrNot = false;
            outNodes.addAll(workGroup.getOut());
            for (WorkNode outNode : outNodes) {
                subStatement.add(mysqlConnector.getOrderUpdate(false, outNode.getTableIndex(),
                        outNode.getTupleIndex()));
            }
        }
    }

    @Override
    public void run() {
        Random r = new Random();
        Connection conn = mysqlConnector.getConn();
        WorkNode work;
        PreparedStatement preparedStatement;
        if (addOrNot) {
            int randomInIndex = r.nextInt(inNodes.size());
            work = inNodes.get(randomInIndex);
            preparedStatement = addStatement.get(randomInIndex);
        } else {
            int randomOutIndex = r.nextInt(outNodes.size());
            work = outNodes.get(randomOutIndex);
            preparedStatement = subStatement.get(randomOutIndex);
        }
        for (int i = 0; i < runCount; i++) {
            try {
                int workPriKey = work.getSubValueList().get(
                        tables[work.getTableIndex()].getRandomKey() - 1);
                preparedStatement.setInt(1, workPriKey);
                if (preparedStatement.executeUpdate() == 0) {
                    System.out.println(preparedStatement.toString());
                    conn.rollback();
                    continue;
                }
                conn.commit();
                logger.trace(work.getTableIndex() + "," + work.getTupleIndex() + "," + workPriKey);
            } catch (SQLException e) {
                e.printStackTrace();
                System.out.println(preparedStatement.toString());
                System.exit(-1);
            }
        }
        count.countDown();
        mysqlConnector.close();
    }
}
