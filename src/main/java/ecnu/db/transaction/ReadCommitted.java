package ecnu.db.transaction;

import ecnu.db.check.WorkNode;
import ecnu.db.utils.MysqlConnector;
import ecnu.db.utils.ZipDistributionList;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;

public class ReadCommitted extends BaseTransaction {

    private static final String UPDATE_NEGATIVE = "update t* set tp* = -1 where tp0 between ? and ?";

    private static final String UPDATE_RECORD = "update t* set checkReadCommitted = ? where tp0= ? and checkReadCommitted >= 0";

    private static final String SELECT_SQL = "select tp* from t* where tp0 =?";


    private String updateNegative;
    private String updateRecord;
    private String selectSQL;

    private PreparedStatement updateNegativePreparedStatement;
    private PreparedStatement updateRecordPreparedStatement;
    private PreparedStatement selectSQLPreparedStatement;

    private ZipDistributionList key;
    private long updateSleepMills;
    private double readWriteRadio;
    private int range;

    public ReadCommitted(WorkNode workNode, long updateSleepMills, double readWriteRadio) {

        this.updateSleepMills = updateSleepMills;
        this.readWriteRadio = readWriteRadio;

        updateNegative = UPDATE_NEGATIVE;
        updateNegative = updateNegative.replaceFirst("\\*", String.valueOf(workNode.getTableIndex()));
        updateNegative = updateNegative.replace("*", String.valueOf(workNode.getColumnIndex()));

        updateRecord = UPDATE_RECORD;
        updateRecord = updateRecord.replaceFirst("\\*", String.valueOf(workNode.getTableIndex()));

        selectSQL = SELECT_SQL;
        selectSQL = selectSQL.replaceFirst("\\*", String.valueOf(workNode.getColumnIndex()));
        selectSQL = selectSQL.replace("*", String.valueOf(workNode.getTableIndex()));

        key = new ZipDistributionList(workNode.getKeys(), true);

        range = Collections.max(workNode.getKeys());
    }

    @Override
    public void makePrepareStatement(MysqlConnector mysqlConnector) throws SQLException {
        this.mysqlConnector = mysqlConnector;
        updateNegativePreparedStatement = mysqlConnector.getPrepareStatement(updateNegative);
        updateRecordPreparedStatement = mysqlConnector.getPrepareStatement(updateRecord);
        selectSQLPreparedStatement = mysqlConnector.getPrepareStatement(selectSQL);
    }

    @Override
    public void execute() throws SQLException {
        if (R.nextDouble() < readWriteRadio) {
            int selectKey = key.getValue();
            selectSQLPreparedStatement.setInt(1, selectKey);
            ResultSet rs = selectSQLPreparedStatement.executeQuery();
            if (rs.next()) {
                updateRecordPreparedStatement.setObject(1, rs.getObject(1));
                updateRecordPreparedStatement.setInt(2, selectKey);
                updateRecordPreparedStatement.executeUpdate();
                mysqlConnector.commit();
            }
            mysqlConnector.rollback();
        } else {
            int min = R.nextInt(range);
            int max = R.nextInt(range);
            if (max < min) {
                int temp = min;
                min = max;
                max = temp;
            }
            updateNegativePreparedStatement.setInt(1, min);
            updateNegativePreparedStatement.setInt(2, max);
            updateNegativePreparedStatement.execute();
            try {
                Thread.sleep(updateSleepMills);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            mysqlConnector.rollback();
        }
    }
}
