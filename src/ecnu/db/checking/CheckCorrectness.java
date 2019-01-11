package ecnu.db.checking;

import ecnu.db.threads.ComputeSum;
import ecnu.db.utils.LoadConfig;

import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;


/**
 * @author wangqingshuai
 * 核心校验类，包含前置求和 后置求和 以及事务执行的相关代码
 */
public class CheckCorrectness {
    private ArrayList<WorkGroup> workGroups;
    private ThreadPoolExecutor threadPoolExecutor;

    public CheckCorrectness(ThreadPoolExecutor threadPoolExecutor) {
        workGroups = LoadConfig.getConfig().getWorkNode();
        for (WorkGroup workGroup : workGroups) {
            if (!workGroup.check()) {
                System.out.println("工作组" + workGroup.getWorkId() + "只有in或者只有out，" +
                        "无法操作，请检查配置文件");
                System.exit(-1);
            }
        }
        this.threadPoolExecutor = threadPoolExecutor;
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
            threadPoolExecutor.submit(computeSums[i]);
        }
        try {
            count.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
