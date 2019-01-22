package ecnu.db.checking;

import ecnu.db.scheme.Table;
import ecnu.db.threads.*;
import ecnu.db.utils.LoadConfig;

import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.stream.IntStream;


/**
 * @author wangqingshuai
 * 核心校验类，包含前置求和 后置求和 以及事务执行的相关代码
 */
public class CheckCorrectness {
    private ArrayList<WorkGroup> workGroups;

    /**
     * 按照配置文件生成转账组
     */
    public CheckCorrectness() {
        workGroups = LoadConfig.getConfig().getWorkNode();
        for (WorkGroup workGroup : workGroups) {
            workGroup.check();
        }
    }

    public void work(Table[] tables) {
        int runCount = LoadConfig.getConfig().getRunCount();
        int[] threadsNum = new int[workGroups.size()];
        for (int i = 0; i < workGroups.size(); i++) {
            threadsNum[i] = LoadConfig.getConfig().getThreadNum(workGroups.get(i).getWorkId());
        }
        int totalNum = IntStream.of(threadsNum).sum();
        CountDownLatch count = new CountDownLatch(totalNum);
        for (int i = 0; i < workGroups.size(); i++) {
            switch (workGroups.get(i).getWorkGroupType()) {
                case remittance:
                    for (int j = 0; j < threadsNum[i]; j++) {
                        RemittanceTransaction remittanceTransaction = new RemittanceTransaction(
                                tables, workGroups.get(i), runCount, count);
                        ThreadPool.getThreadPoolExecutor().submit(remittanceTransaction);
                    }
                    break;
                case function:
                    for (int j = 0; j < threadsNum[i]; j++) {
                        FunctionTransaction functionTransaction = new FunctionTransaction(
                                tables, workGroups.get(i), runCount, count);
                        ThreadPool.getThreadPoolExecutor().submit(functionTransaction);
                    }
                    break;
                case order:
                    for (int j = 0; j < threadsNum[i]; j++) {
                        OrderTransaction orderTransaction = new OrderTransaction(
                                tables, workGroups.get(i), runCount, count);
                        ThreadPool.getThreadPoolExecutor().submit(orderTransaction);
                    }
                    break;
                default:
                    try {
                        throw new Exception("没有匹配到工作组");
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
            }
        }
        try {
            count.await();
            System.out.println("全部事务执行完毕！");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    public void printWorkGroup() {
        for (WorkGroup workGroup : workGroups) {
            System.out.println(workGroup);
        }
    }

    public void computeBeginSum() {
        computeSum(true);
    }

    public void computeEndSum() {
        computeSum(false);
    }

    private void computeSum(Boolean isBegin) {
        ComputeSum[] computeSums = new ComputeSum[workGroups.size()];
        CountDownLatch count = new CountDownLatch(workGroups.size());
        for (int i = 0; i < workGroups.size(); i++) {
            computeSums[i] = new ComputeSum(workGroups.get(i), count, isBegin);
            ThreadPool.getThreadPoolExecutor().submit(computeSums[i]);
        }
        try {
            count.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void checkCorrect() {
        for (WorkGroup workGroup : workGroups) {
            workGroup.checkCorrect();
        }
    }
}
