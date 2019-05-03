package ecnu.db.transaction;

import ecnu.db.check.CheckNode;
import ecnu.db.utils.MysqlConnector;
import ecnu.db.utils.ZipDistributionList;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;

public class RepeatableRead extends BaseTransaction {
    private static final String UPDATE_INFLUENCE_READ = "update t* set tp*= tp* +1 where tp0 between ? and ?";

    private static final String SELECT_SQL = "select tp* from t* where tp0 = ?";

    private static final String UPDATE_REPEATABLE_READ =
            "update t* set checkRepeatableRead= checkRepeatableRead + ? where tp0 = ?";

    private String updateInfluenceRead;
    private String selectSQL;
    private String updateRepeatableRead;

    private PreparedStatement updateInfluenceReadPreparedStatement;
    private PreparedStatement selectSQLPreparedStatement;
    private PreparedStatement updateRepeatableReadPreparedStatement;


    private ZipDistributionList key;
    private int range;

    private long selectSleepMills;
    private double readWriteRadio;

    public RepeatableRead(CheckNode checkNode, long selectSleepMills, double readWriteRadio) {
        updateInfluenceRead = UPDATE_INFLUENCE_READ;
        updateInfluenceRead = updateInfluenceRead.replaceFirst("\\*", String.valueOf(checkNode.getTableIndex()));
        updateInfluenceRead = updateInfluenceRead.replace("*", String.valueOf(checkNode.getColumnIndex()));

        selectSQL = SELECT_SQL;
        selectSQL = selectSQL.replaceFirst("\\*", String.valueOf(checkNode.getColumnIndex()));
        selectSQL = selectSQL.replaceFirst("\\*", String.valueOf(checkNode.getTableIndex()));

        updateRepeatableRead = UPDATE_REPEATABLE_READ;
        updateRepeatableRead = updateRepeatableRead.replaceFirst("\\*", String.valueOf(checkNode.getTableIndex()));

        key = new ZipDistributionList(checkNode.getKeys(), true);
        range = Collections.max(checkNode.getKeys());
        this.selectSleepMills = selectSleepMills;
        this.readWriteRadio = readWriteRadio;
    }

    @Override
    public void makePrepareStatement(MysqlConnector mysqlConnector) throws SQLException {
        this.mysqlConnector = mysqlConnector;
        updateInfluenceReadPreparedStatement = mysqlConnector.getPrepareStatement(updateInfluenceRead);
        selectSQLPreparedStatement = mysqlConnector.getPrepareStatement(selectSQL);
        updateRepeatableReadPreparedStatement = mysqlConnector.getPrepareStatement(updateRepeatableRead);
    }

    @Override
    public void execute() throws SQLException {
        if (R.nextDouble() < readWriteRadio) {
            int selectKey = key.getValue();
            selectSQLPreparedStatement.setInt(1, selectKey);
            ResultSet rs = selectSQLPreparedStatement.executeQuery();
            if (rs.next()) {
                Double firstValue = rs.getDouble(1);
                try {
                    Thread.sleep(selectSleepMills);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                rs = selectSQLPreparedStatement.executeQuery();
                rs.next();
                Double secondValue = rs.getDouble(1);
                updateRepeatableReadPreparedStatement.setObject(1, Math.abs(firstValue - secondValue));
                updateRepeatableReadPreparedStatement.setInt(2, selectKey);
                updateRepeatableReadPreparedStatement.execute();
                mysqlConnector.commit();
            }
        } else {
            int min = R.nextInt(range);
            int max = R.nextInt(range);
            if (max < min) {
                int temp = min;
                min = max;
                max = temp;
            }
            updateInfluenceReadPreparedStatement.setInt(1, min);
            updateInfluenceReadPreparedStatement.setInt(2, max);
            updateInfluenceReadPreparedStatement.execute();
            mysqlConnector.commit();
        }
    }
}
