package ecnu.db.transaction;

import ecnu.db.scheme.Table;
import ecnu.db.utils.MysqlConnector;
import org.apache.logging.log4j.LogManager;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.CountDownLatch;

public class UpdateNoCommitTransaction implements Runnable {
    private Table table;
    private int tupleIndex;
    private int runCount;
    private CountDownLatch count;
    private MysqlConnector mysqlConnector = new MysqlConnector();

    public UpdateNoCommitTransaction(Table table, int tupleIndex, int runCount, CountDownLatch count) {
        this.table = table;
        this.tupleIndex = tupleIndex;
        this.runCount = runCount;
        this.count = count;
    }

    @Override
    public void run() {
        PreparedStatement updateNoCommitPrepareStatement = mysqlConnector.
                getUpdateNoCommitStatement(table.getTableIndex(), tupleIndex);
        Connection conn = mysqlConnector.getConn();
        for (int i = 0; i < runCount; i++) {
            double min = table.getRandomValue(tupleIndex);
            double max = table.getRandomValue(tupleIndex);
            if (max < min) {
                max = max + min;
                min = max - min;
                max = max - min;
            }
            assert max >= min;

            try {
                updateNoCommitPrepareStatement.setObject(1, min);
                updateNoCommitPrepareStatement.setObject(2, max);
                updateNoCommitPrepareStatement.executeUpdate();
                Thread.sleep(2000);
                conn.rollback();
            } catch (SQLException | InterruptedException e) {
                LogManager.getLogger().error(e);
            }
        }
        count.countDown();
        mysqlConnector.close();

    }
}
