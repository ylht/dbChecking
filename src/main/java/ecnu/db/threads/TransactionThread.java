package ecnu.db.threads;

import ecnu.db.checking.CheckType;
import ecnu.db.checking.WorkGroup;
import ecnu.db.scheme.Table;
import ecnu.db.threads.pool.ThreadPool;
import ecnu.db.transaction.BaseTransaction;
import ecnu.db.transaction.FunctionTransaction;
import ecnu.db.transaction.OrderTransaction;
import ecnu.db.transaction.RemittanceTransaction;
import ecnu.db.utils.MysqlConnector;
import org.apache.logging.log4j.LogManager;

import java.util.ArrayList;

public class TransactionThread implements Runnable {
    private ArrayList<BaseTransaction> transactions = new ArrayList<>();

    public TransactionThread(Table[] tables, ArrayList<WorkGroup> workGroups, CheckType checkType) {
        MysqlConnector mysqlConnector = new MysqlConnector();
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

    }
}
