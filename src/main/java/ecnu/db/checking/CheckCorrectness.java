package ecnu.db.checking;

import ecnu.db.scheme.Table;
import ecnu.db.threads.ComputeSum;
import ecnu.db.threads.pool.ThreadPool;
import ecnu.db.transaction.*;
import ecnu.db.utils.LoadConfig;
import org.apache.logging.log4j.LogManager;

import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.stream.IntStream;


/**
 * @author wangqingshuai
 * 核心校验类，包含前置求和 后置求和 以及事务执行的相关代码
 */
public class CheckCorrectness {
    private ArrayList<WorkGroup> workGroups;
    private CheckType checkType;

    /**
     * 按照配置文件生成转账组
     */
    public CheckCorrectness(CheckType checkType) {
        this.checkType = checkType;
        workGroups = LoadConfig.getConfig().getWorkNode();
        for (WorkGroup workGroup : workGroups) {
            workGroup.check();
        }
    }

    private int getAllThreadNum(int[] threadsNum) {
        int totalNum = IntStream.of(threadsNum).sum();
        if (checkType.isScan()) {
            if (checkType.isScanCheckReadUncommited()) {
                totalNum += LoadConfig.getConfig().getScanThreadNum() * 2;
            } else {
                totalNum += LoadConfig.getConfig().getScanThreadNum() * 3;
            }
        }
        return totalNum;
    }

    private void workScanWork(CountDownLatch count, Table[] tables) {
        Random r = new Random();
        for (int i = 0; i < LoadConfig.getConfig().getScanThreadNum(); i++) {
            int randomTable = r.nextInt(tables.length);
            int randomTuple = r.nextInt(tables[randomTable].getTableColSizeExceptKey()) + 1;
            if (checkType.isScanCheckReadUncommited()) {
                UpdateNoCommitTransaction updateNoCommitTransaction = new UpdateNoCommitTransaction(tables[randomTable],
                        randomTuple, LoadConfig.getConfig().getUpdateNoCommitedRunCount(), count);
                ThreadPool.getThreadPoolExecutor().submit(updateNoCommitTransaction);

            } else {
                ChangeTableSize insertTable = new ChangeTableSize(true, tables[randomTable],
                        LoadConfig.getConfig().getRunCount(), count);
                ThreadPool.getThreadPoolExecutor().submit(insertTable);
                ChangeTableSize deleteTable = new ChangeTableSize(false, tables[randomTable],
                        LoadConfig.getConfig().getRunCount(), count);
                ThreadPool.getThreadPoolExecutor().submit(deleteTable);
            }
            ScanTransaction scanTransaction = new ScanTransaction(tables[randomTable],
                    randomTuple, LoadConfig.getConfig().getRunCount(),
                    count, checkType.isScanCheckReadUncommited());
            ThreadPool.getThreadPoolExecutor().submit(scanTransaction);

        }
    }

    public void work(Table[] tables) {
        System.out.println("开始执行事务");
        int runCount = LoadConfig.getConfig().getRunCount();
        int[] threadsNum = new int[workGroups.size()];
        for (int i = 0; i < workGroups.size(); i++) {
            threadsNum[i] = LoadConfig.getConfig().getThreadNum();
        }
        CountDownLatch count = new CountDownLatch(getAllThreadNum(threadsNum));



        for (int i = 0; i < workGroups.size(); i++) {
            switch (workGroups.get(i).getWorkGroupType()) {
                case remittance:
                    if (checkType.isUpdateWithSelect()) {
                        for (int j = 0; j < threadsNum[i]; j++) {
                            RemittanceBySelectTransaction remittanceTransaction = new RemittanceBySelectTransaction(
                                    tables, workGroups.get(i), runCount, count, checkType.isForUpdate());
                            ThreadPool.getThreadPoolExecutor().submit(remittanceTransaction);
                        }
                    } else {
                        for (int j = 0; j < threadsNum[i]; j++) {
                            RemittanceTransaction remittanceTransaction = new RemittanceTransaction(tables,
                                    workGroups.get(i), runCount, count);
                            ThreadPool.getThreadPoolExecutor().submit(remittanceTransaction);
                        }
                    }
                    break;
                case function:
                    if (checkType.isUpdateWithSelect()) {
                        for (int j = 0; j < threadsNum[i]; j++) {
                            FunctionBySelectTransaction functionTransaction = new FunctionBySelectTransaction(
                                    tables, workGroups.get(i), runCount, count, checkType.isForUpdate());
                            ThreadPool.getThreadPoolExecutor().submit(functionTransaction);
                        }
                    } else {
                        for (int j = 0; j < threadsNum[i]; j++) {
                            FunctionTransaction functionTransaction = new FunctionTransaction(
                                    tables, workGroups.get(i), runCount, count);
                            ThreadPool.getThreadPoolExecutor().submit(functionTransaction);
                        }
                    }

                    break;
                case order:
                    if (checkType.isUpdateWithSelect()) {
                        for (int j = 0; j < threadsNum[i]; j++) {
                            OrderBySelectTransaction orderTransaction = new OrderBySelectTransaction(
                                    tables, workGroups.get(i), runCount, count, checkType.isForUpdate());
                            ThreadPool.getThreadPoolExecutor().submit(orderTransaction);
                        }
                    } else {
                        for (int j = 0; j < threadsNum[i]; j++) {
                            OrderTransaction orderTransaction = new OrderTransaction(
                                    tables, workGroups.get(i), runCount, count);
                            ThreadPool.getThreadPoolExecutor().submit(orderTransaction);
                        }
                    }

                    break;
                default:
                    try {
                        throw new Exception("没有匹配到工作组");
                    } catch (Exception e) {
                        LogManager.getLogger().error(e);
                    }
            }
        }
        if (checkType.isScan()) {
            workScanWork(count, tables);
        }

        try {
            count.await();
            System.out.println("全部事务执行完毕！");
        } catch (InterruptedException e) {
            LogManager.getLogger().error(e);
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
            LogManager.getLogger().error(e);
        }
    }

    public void checkCorrect() {
        for (WorkGroup workGroup : workGroups) {
            workGroup.checkCorrect();
        }
    }
}
