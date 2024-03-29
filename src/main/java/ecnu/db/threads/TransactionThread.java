package ecnu.db.threads;

import ecnu.db.transaction.BaseTransaction;
import ecnu.db.utils.DatabaseConnector;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

/**
 * @author wangqingshuai
 */
public class TransactionThread implements Runnable {
    private final Random r = new Random();
    private final ArrayList<BaseTransaction> transactions = new ArrayList<>();
    private final int runCount;
    private final CountDownLatch count;
    private final DatabaseConnector databaseConnector;
    private final int threadID;


    public TransactionThread(int threadID, ArrayList<BaseTransaction> transactions,
                             int runCount, CountDownLatch count) {
        this.runCount = runCount;
        this.count = count;
        this.threadID = threadID;
        databaseConnector = new DatabaseConnector();

        for (BaseTransaction transaction : transactions) {
            try {
                this.transactions.add((BaseTransaction) transaction.clone());
            } catch (CloneNotSupportedException e) {
                e.printStackTrace();
            }
        }
        for (BaseTransaction transaction : this.transactions) {
            try {
                transaction.makePrepareStatement(databaseConnector);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void run() {
        for (int i = 0; i < runCount; i++) {
            int randomIndex = r.nextInt(transactions.size());
            try {
                databaseConnector.setAutoCommitFalse();
                transactions.get(randomIndex).execute();
            } catch (Exception e) {
                if (!"Deadlock found when trying to get lock; try restarting transaction".equals(e.getMessage())) {
                    e.printStackTrace();
                }
                try {
                    databaseConnector.rollback();
                } catch (SQLException ex) {
                    ex.printStackTrace();
                }
            }
            if (i % 100 == 0) {
                System.out.println("第" + threadID + "号线程执行到第" + i + "次");
            }
        }
        count.countDown();
        databaseConnector.close();
    }
}
