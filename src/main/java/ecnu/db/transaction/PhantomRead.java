package ecnu.db.transaction;

import ecnu.db.check.WorkNode;
import ecnu.db.utils.MysqlConnector;
import ecnu.db.utils.ZipDistributionList;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;

public class PhantomRead extends BaseTransaction {
    private static final String INSERT_SQL = "insert into t* （tp0,tp*）values(?,?)";
    private static final String DELETE_SQL = "delete from t* where tp0=?";
    private static final String UPDATE_SQL = "update t* set tp* = tp* + 1 where tp0 =?";
    private static final String SELECT_SQL = "select tp0,tp* from t* where tp0 between ? and ? order by tp0";

    private static final String INSERT_PHANTOM_READ = "insert phantom_read_record values(*,?)";

    private String insertSQL;
    private String deleteSQL;
    private String updateSQL;
    private String selectSQL;
    private String insertPhantomRead;

    private PreparedStatement insertSQLPreparedStatement;
    private PreparedStatement deleteSQLPreparedStatement;
    private PreparedStatement updateSQLPreparedStatement;
    private PreparedStatement selectSQLPreparedStatement;
    private PreparedStatement insertPhantomReadPreparedStatement;

    private ZipDistributionList key;
    private int range;
    private long sleepMills;
    private double readWriteRadio;

    public PhantomRead(WorkNode workNode, long sleepMills, double readWriteRadio) {
        insertSQL = INSERT_SQL;
        insertSQL = insertSQL.replaceFirst("\\*", String.valueOf(workNode.getTableIndex()));
        insertSQL = insertSQL.replaceFirst("\\*", String.valueOf(workNode.getColumnIndex()));

        deleteSQL = DELETE_SQL;
        deleteSQL = deleteSQL.replaceFirst("\\*", String.valueOf(workNode.getTableIndex()));

        updateSQL = UPDATE_SQL;
        updateSQL = updateSQL.replaceFirst("\\*", String.valueOf(workNode.getTableIndex()));
        updateSQL = updateSQL.replace("*", String.valueOf(workNode.getColumnIndex()));

        selectSQL = SELECT_SQL;
        selectSQL = selectSQL.replaceFirst("\\*", String.valueOf(workNode.getColumnIndex()));
        selectSQL = selectSQL.replaceFirst("\\*", String.valueOf(workNode.getTableIndex()));

        insertPhantomRead = INSERT_PHANTOM_READ;
        insertPhantomRead = insertPhantomRead.replaceFirst("\\*", String.valueOf(workNode.getTableIndex()));

        key = new ZipDistributionList(workNode.getKeys(), true);
        range = Collections.max(workNode.getKeys());
        this.sleepMills = sleepMills;
        this.readWriteRadio = readWriteRadio;
    }


    @Override
    public void makePrepareStatement(MysqlConnector mysqlConnector) throws SQLException {
        this.mysqlConnector = mysqlConnector;
        insertSQLPreparedStatement = mysqlConnector.getPrepareStatement(insertSQL);
        deleteSQLPreparedStatement = mysqlConnector.getPrepareStatement(deleteSQL);
        updateSQLPreparedStatement = mysqlConnector.getPrepareStatement(updateSQL);
        selectSQLPreparedStatement = mysqlConnector.getPrepareStatement(selectSQL);
        insertPhantomReadPreparedStatement = mysqlConnector.getPrepareStatement(insertPhantomRead);
    }

    @Override
    public void execute() throws SQLException {
        if (R.nextDouble() < readWriteRadio) {
            int min = R.nextInt(range);
            int max = R.nextInt(range);
            if (max < min) {
                int temp = min;
                min = max;
                max = temp;
            }
            selectSQLPreparedStatement.setInt(1, min);
            selectSQLPreparedStatement.setInt(2, max);
            ResultSet firstResultSet = selectSQLPreparedStatement.executeQuery();
            if (firstResultSet.next()) {
                try {
                    Thread.sleep(sleepMills);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                ResultSet secondResultSet = selectSQLPreparedStatement.executeQuery();
                mysqlConnector.commit();
                int firstRowCount = 0;
                if (firstResultSet.last()) {
                    firstRowCount = firstResultSet.getRow();
                    firstResultSet.beforeFirst();
                }
                int secondRowCount = 0;
                if (secondResultSet.last()) {
                    secondRowCount = secondResultSet.getRow();
                    secondResultSet.beforeFirst();
                }
                if (firstRowCount > secondRowCount) {
                    insertPhantomReadPreparedStatement.setInt(1, 0);
                    insertPhantomReadPreparedStatement.executeUpdate();
                    mysqlConnector.commit();
                } else {
                    insertPhantomReadPreparedStatement.setInt(1, 1);
                    insertPhantomReadPreparedStatement.executeUpdate();
                    mysqlConnector.commit();
                }
                while (firstResultSet.next() && secondResultSet.next()) {
                    if (firstResultSet.getInt(1) != secondResultSet.getInt(1) ||
                            !firstResultSet.getObject(2).equals(secondResultSet.getObject(2))) {
                        insertPhantomReadPreparedStatement.setInt(1, 2);
                        insertPhantomReadPreparedStatement.executeUpdate();
                        mysqlConnector.commit();
                        break;
                    }
                }
            }
        } else {
            int workKey = key.getValue();
            switch (R.nextInt(3)) {
                case 0:
                    insertSQLPreparedStatement.setInt(1, workKey);
                    insertSQLPreparedStatement.setObject(1, 1);
                    insertSQLPreparedStatement.executeUpdate();
                    break;
                case 1:
                    deleteSQLPreparedStatement.setInt(1, workKey);
                    deleteSQLPreparedStatement.executeUpdate();
                    break;
                case 2:
                    updateSQLPreparedStatement.setInt(1, workKey);
                    updateSQLPreparedStatement.executeUpdate();
                    break;
                default:
                    System.out.println("没有改操作类型");
            }
            mysqlConnector.commit();
        }
    }
}
