package ecnu.db.transaction;

import ecnu.db.check.CheckNode;
import ecnu.db.utils.DatabaseConnector;
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

    private final ZipDistributionList key;
    private final long updateSleepMills;
    private final double readWriteRadio;
    private final int range;

    public ReadCommitted(CheckNode checkNode, long updateSleepMills, double readWriteRadio) {

        this.updateSleepMills = updateSleepMills;
        this.readWriteRadio = readWriteRadio;

        updateNegative = UPDATE_NEGATIVE;
        updateNegative = updateNegative.replaceFirst("\\*", String.valueOf(checkNode.getTableIndex()));
        updateNegative = updateNegative.replace("*", String.valueOf(checkNode.getColumnIndex()));

        updateRecord = UPDATE_RECORD;
        updateRecord = updateRecord.replaceFirst("\\*", String.valueOf(checkNode.getTableIndex()));

        selectSQL = SELECT_SQL;
        selectSQL = selectSQL.replaceFirst("\\*", String.valueOf(checkNode.getColumnIndex()));
        selectSQL = selectSQL.replace("*", String.valueOf(checkNode.getTableIndex()));

        key = new ZipDistributionList(checkNode.getKeys(), true);

        range = Collections.max(checkNode.getKeys());
    }

    @Override
    public void makePrepareStatement(DatabaseConnector databaseConnector) throws SQLException {
        this.databaseConnector = databaseConnector;
        updateNegativePreparedStatement = databaseConnector.getPrepareStatement(updateNegative);
        updateRecordPreparedStatement = databaseConnector.getPrepareStatement(updateRecord);
        selectSQLPreparedStatement = databaseConnector.getPrepareStatement(selectSQL);
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
                databaseConnector.commit();
            }
            databaseConnector.rollback();
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
            databaseConnector.rollback();
        }
    }
}
