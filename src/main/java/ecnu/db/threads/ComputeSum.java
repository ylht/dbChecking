package ecnu.db.threads;

import ecnu.db.checking.WorkGroup;
import ecnu.db.utils.MysqlConnector;

import java.util.concurrent.CountDownLatch;

/**
 * @author wangqingshuai
 * 计算数据和的线程
 */
public class ComputeSum implements Runnable {
    private WorkGroup workGroup;
    private CountDownLatch count;
    private Boolean isBegin;

    public ComputeSum(WorkGroup workGroup, CountDownLatch count, boolean isBegin) {
        this.workGroup = workGroup;
        this.count = count;
        this.isBegin = isBegin;
    }

    @Override
    public void run() {
        MysqlConnector mysqlConnector = new MysqlConnector();
        workGroup.computeAllSum(isBegin, mysqlConnector);
        count.countDown();
        mysqlConnector.close();
    }
}
