package ecnu.db.transaction;

import ecnu.db.scheme.Table;
import ecnu.db.utils.LoadConfig;
import ecnu.db.utils.MysqlConnector;
import org.apache.logging.log4j.LogManager;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

public class ChangeTableSize implements Runnable {
    private boolean insert;
    private Table table;
    private int randomSize;
    private PreparedStatement changeTablePrepareStatement;
    private int runCount;
    private CountDownLatch count;
    private MysqlConnector mysqlConnector = new MysqlConnector();

    public ChangeTableSize(boolean insert, Table table, int runCount, CountDownLatch count) {
        this.insert = insert;
        this.table = table;
        this.runCount = runCount;
        this.count = count;
        randomSize = table.getTableSize() / LoadConfig.getConfig().getKeyRange();
        if (insert) {
            changeTablePrepareStatement = mysqlConnector.getInsertStatement(
                    table.getTableIndex(), table.getTableColSizeExceptKey());
        } else {
            changeTablePrepareStatement = mysqlConnector.getDeleteStatement(table.getTableIndex());
        }
    }

    @Override
    public void run() {
        Connection conn = mysqlConnector.getConn();
        Random r = new Random();
        int keyRange = LoadConfig.getConfig().getKeyRange();
        if (insert) {
            for (int j = 0; j < runCount; j++) {
                Object[] values = table.getInsertValue();
                try {
                    values[0] = r.nextInt(randomSize) * keyRange;
                    int i = 1;
                    for (Object value : values) {
                        changeTablePrepareStatement.setObject(i++, value);
                    }
                    changeTablePrepareStatement.executeUpdate();
                    conn.commit();
                } catch (SQLException e) {
                    System.out.println(changeTablePrepareStatement);
                    LogManager.getLogger().error(e);
                }
            }
        } else {
            for (int j = 0; j < runCount; j++) {
                try {
                    changeTablePrepareStatement.setInt(1, r.nextInt(randomSize) * keyRange + 1);
                    changeTablePrepareStatement.executeUpdate();
                    conn.commit();
                } catch (SQLException e) {
                    System.out.println(changeTablePrepareStatement);
                    LogManager.getLogger().error(e);
                }
            }
        }
        count.countDown();
        mysqlConnector.close();
    }
}
