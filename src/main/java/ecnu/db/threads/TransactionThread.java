package ecnu.db.threads;

import ecnu.db.scheme.Table;
import ecnu.db.transaction.*;
import ecnu.db.utils.MysqlConnector;
import ecnu.db.work.CheckType;
import ecnu.db.work.group.BaseWorkGroup;
import ecnu.db.work.group.PhantomReadWorkGroup;
import org.apache.logging.log4j.LogManager;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

/**
 * @author wangqingshuai
 */
public class TransactionThread implements Runnable {
    private static Random r = new Random();
    private ArrayList<BaseTransaction> transactions = new ArrayList<>();
    private int runCount;
    private CountDownLatch count;
    private MysqlConnector mysqlConnector;

    public TransactionThread(Table[] tables, ArrayList<BaseWorkGroup> workGroups, CheckType checkType,
                             int runCount, CountDownLatch count) throws SQLException {
        this.runCount = runCount;
        this.count = count;

        mysqlConnector = new MysqlConnector();
        for (BaseWorkGroup workGroup : workGroups) {
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
                            workGroup, mysqlConnector);
                    transactions.add(writeSkewTransaction);
                    break;
                case noCommit:
                    BaseTransaction updateNoCommitTransaction = new UpdateNoCommitTransaction(
                            workGroup, mysqlConnector);
                    transactions.add(updateNoCommitTransaction);
                    BaseTransaction selectNoCommitTransaction = new SelectNoCommitTransaction(
                            workGroup, mysqlConnector);
                    transactions.add(selectNoCommitTransaction);
                    break;
                case repeatableRead:
                    BaseTransaction repeatableReadTransaction = new RepeatableReadTransaction(
                            workGroup, mysqlConnector);
                    transactions.add(repeatableReadTransaction);
                    break;
                case phantomRead:
                    if (((PhantomReadWorkGroup) workGroup).isChangeTableSize()) {
                        BaseTransaction changeTableSize = new ChangeTableSize(tables,
                                workGroup, mysqlConnector);
                        transactions.add(changeTableSize);
                    }
                    BaseTransaction scanTransaction = new ScanTransaction(tables,
                            workGroup, mysqlConnector);
                    transactions.add(scanTransaction);
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
            int randomIndex = r.nextInt(transactions.size());
            try {
                transactions.get(randomIndex).execute();
            } catch (SQLException e) {
                LogManager.getLogger().error(e);
            }
            if (i % 100 == 0) {
                System.out.println(i);
            }
        }
        count.countDown();
        mysqlConnector.close();
    }
}
