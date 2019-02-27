package ecnu.db.transaction;

import ecnu.db.scheme.Table;
import ecnu.db.utils.MysqlConnector;
import org.apache.logging.log4j.LogManager;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;

public class ScanTransaction implements Runnable {
    private Table table;
    private int tupleIndex;
    private int runCount;
    private CountDownLatch count;
    private MysqlConnector mysqlConnector;
    private PreparedStatement scanStatement;
    private boolean scanCheckReadUncommited;

    public ScanTransaction(Table table, int tupleIndex, int runCount,
                           CountDownLatch count, boolean scanCheckReadUncommited) {
        this.table = table;
        this.tupleIndex = tupleIndex;
        this.runCount = runCount;
        this.count = count;
        mysqlConnector = new MysqlConnector();
        this.scanCheckReadUncommited = scanCheckReadUncommited;
    }

    private ArrayList<Integer> datas(double max, double min) throws SQLException {
        scanStatement.setDouble(1, min);
        scanStatement.setDouble(2, max);
        ResultSet rs = scanStatement.executeQuery();
        ArrayList<Integer> datas = new ArrayList<>();

        while (rs.next()) {
            datas.add(rs.getInt(1));
        }
        return datas;
    }

    @Override
    public void run() {
        Connection conn = mysqlConnector.getConn();
        scanStatement = mysqlConnector.getScanStatement(table.getTableIndex(), tupleIndex);
        PreparedStatement updateStatement = mysqlConnector.getUpdateAllStatement(table.getTableIndex(), tupleIndex);
        int i;
        if (scanCheckReadUncommited) {
            scan:
            for (i = 0; i < runCount; i++) {
                double min = table.getRandomValue(tupleIndex);
                double max = table.getRandomValue(tupleIndex);
                if (max < min) {
                    max = max + min;
                    min = max - min;
                    max = max - min;
                }
                assert max >= min;
                try {
                    ArrayList<Integer> data = datas(max, min);
                    for (Integer integer : data) {
                        if (integer < 0) {
                            System.out.println("scan两次验证结果不同");
                            break scan;
                        }
                    }
                } catch (SQLException e) {
                    LogManager.getLogger().error(e);
                }
            }
        } else {
            scan:
            for (i = 0; i < runCount; i++) {
                double min = table.getRandomValue(tupleIndex);
                double max = table.getRandomValue(tupleIndex);
                if (max < min) {
                    max = max + min;
                    min = max - min;
                    max = max - min;
                }
                assert max >= min;
                try {
                    ArrayList<Integer> oldData = datas(max, min);
                    //保证间隔 使其有可变更数据的空间
                    Thread.sleep(1000);
                    //整体数据加1，以验证mysql版本的幻读
                    updateStatement.setObject(1, min);
                    updateStatement.setObject(2, max);
                    updateStatement.executeUpdate();
                    ArrayList<Integer> newData = datas(max + 1, min + 1);
                    conn.rollback();
                    //开始在本地验证结果集的正确性
                    assert oldData.size() == newData.size();
                    Collections.sort(oldData);
                    Collections.sort(newData);
                    for (int j = 0; j < oldData.size(); j++) {
                        if (!oldData.get(j).equals(newData.get(j))) {
                            System.out.println("scan两次验证结果不同");
                            break scan;
                        }
                    }
                } catch (SQLException | InterruptedException e) {
                    LogManager.getLogger().error(e);
                }
            }
        }
        if (i == runCount) {
            System.out.println("scan验证通过");
        }

        count.countDown();
        mysqlConnector.close();


    }
}
