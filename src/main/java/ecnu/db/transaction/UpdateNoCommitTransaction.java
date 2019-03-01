package ecnu.db.transaction;

import ecnu.db.scheme.Table;
import ecnu.db.utils.MysqlConnector;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.CountDownLatch;

public class UpdateNoCommitTransaction extends BaseTransaction {
    private Table table;
    private int tupleIndex;
    private int runCount;
    private CountDownLatch count;

    public UpdateNoCommitTransaction(Table table, int tupleIndex, MysqlConnector mysqlConnector) {
        super(mysqlConnector, false);
        this.table = table;
        this.tupleIndex = tupleIndex;
        this.runCount = runCount;
        this.count = count;
    }

    @Override
    public void execute() throws SQLException {
        PreparedStatement updateNoCommitPrepareStatement = mysqlConnector.
                getUpdateNoCommitStatement(table.getTableIndex(), tupleIndex);
        for (int i = 0; i < runCount; i++) {
            double min = table.getRandomValue(tupleIndex);
            double max = table.getRandomValue(tupleIndex);
            if (max < min) {
                max = max + min;
                min = max - min;
                max = max - min;
            }
            assert max >= min;


            updateNoCommitPrepareStatement.setObject(1, min);
            updateNoCommitPrepareStatement.setObject(2, max);
            updateNoCommitPrepareStatement.executeUpdate();
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            mysqlConnector.rollback();

        }
        count.countDown();
        mysqlConnector.close();

    }
}
