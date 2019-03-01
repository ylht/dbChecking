package ecnu.db.threads;

import ecnu.db.core.CheckType;
import ecnu.db.core.WorkGroup;
import ecnu.db.scheme.Table;
import ecnu.db.transaction.*;
import ecnu.db.utils.MysqlConnector;
import org.apache.logging.log4j.LogManager;

import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

public class TransactionThread implements Runnable {
    private static Random r=new Random();
    private ArrayList<BaseTransaction> transactions = new ArrayList<>();
    private int runCount;
    private CountDownLatch count;
    private MysqlConnector mysqlConnector;
    public TransactionThread(Table[] tables, ArrayList<WorkGroup> workGroups, CheckType checkType,
                             int runCount,CountDownLatch count) {
        this.runCount=runCount;
        this.count=count;

        mysqlConnector = new MysqlConnector();
        for (WorkGroup workGroup : workGroups) {
            switch (workGroup.getWorkGroupType()) {
                case remittance:
                    if (checkType.isUpdateWithSelect()) {
                        BaseTransaction remittanceTransaction = new RemittanceTransaction(tables,
                                workGroup, mysqlConnector, checkType.isForUpdate());
                        transactions.add(remittanceTransaction);
                    } else {
                        BaseTransaction remittanceTransaction = new RemittanceTransaction(tables,
                                workGroup, mysqlConnector);
                        transactions.add(remittanceTransaction);
                    }
                    break;
                case function:
                    if (checkType.isUpdateWithSelect()) {
                        BaseTransaction functionTransaction = new FunctionTransaction(tables,
                                workGroup, mysqlConnector, checkType.isForUpdate());
                        transactions.add(functionTransaction);
                    } else {
                        BaseTransaction functionTransaction = new FunctionTransaction(tables,
                                workGroup, mysqlConnector);
                        transactions.add(functionTransaction);
                    }

                    break;
                case order:
                    if (checkType.isUpdateWithSelect()) {
                        BaseTransaction orderTransaction = new OrderTransaction(tables,
                                workGroup, mysqlConnector, checkType.isForUpdate());
                        transactions.add(orderTransaction);

                    } else {
                        BaseTransaction orderTransaction = new OrderTransaction(tables,
                                workGroup, mysqlConnector);
                        transactions.add(orderTransaction);
                    }

                    break;
                case writeSkew:
                    BaseTransaction writeSkewTransaction = new WriteSkewTransaction(tables,
                            workGroup,mysqlConnector);
                    transactions.add(writeSkewTransaction);
                    break;
                default:
                    try {
                        throw new Exception("没有匹配到工作组");
                    } catch (Exception e) {
                        LogManager.getLogger().error(e);
                    }
            }
        }

    }

    @Override
    public void run() {
        for (int i = 0; i < runCount; i++) {
            int randomIndex=r.nextInt(transactions.size());
            transactions.get(randomIndex).execute();
        }
        count.countDown();
        mysqlConnector.close();
    }
}
