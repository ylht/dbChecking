package ecnu.db.transaction;

import ecnu.db.check.WorkNode;
import ecnu.db.utils.MysqlConnector;
import ecnu.db.utils.ZipDistributionList;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

public class WriteSkew extends BaseTransaction {
    private static final int K = 10;

    private static final String SELECT_SQL = "select tp* from t* where tp0 =?";
    private static final String UPDATE_SQL = "update t* set tp* = tp* - ? where tp0 =? and tp* > ?";

    private String[] selectSQLs;
    private String[] updateSQLs;

    private PreparedStatement[] selectPreparedStatements;
    private PreparedStatement[] updatePreparedStatements;

    private ZipDistributionList key;

    public WriteSkew(ArrayList<WorkNode> workNodes) {
        assert workNodes.size() == 2;
        assert workNodes.get(0).getTableIndex() == workNodes.get(1).getTableIndex();

        selectSQLs = new String[workNodes.size()];
        updateSQLs = new String[workNodes.size()];
        for (int i = 0; i < workNodes.size(); i++) {
            selectSQLs[i] = SELECT_SQL;
            selectSQLs[i] = selectSQLs[i].replaceFirst("\\*", String.valueOf(workNodes.get(0).getColumnIndex()));
            selectSQLs[i] = selectSQLs[i].replaceFirst("\\*", String.valueOf(workNodes.get(0).getTableIndex()));

            updateSQLs[i] = UPDATE_SQL;
            updateSQLs[i] = updateSQLs[i].replaceFirst("\\*", String.valueOf(workNodes.get(0).getTableIndex()));
            updateSQLs[i] = updateSQLs[i].replace("*", String.valueOf(workNodes.get(0).getColumnIndex()));
        }
        key = new ZipDistributionList(workNodes.get(0).getKeys(), true);
    }

    @Override
    public void makePrepareStatement(MysqlConnector mysqlConnector) throws SQLException {
        selectPreparedStatements = getPreparedStatements(selectSQLs);
        updatePreparedStatements = getPreparedStatements(updateSQLs);
    }

    @Override
    public void execute() throws SQLException {
        int workKey = key.getValue();
        int selectIndex = R.nextInt(selectPreparedStatements.length);
        int updateIndex = selectPreparedStatements.length - selectIndex;
        selectPreparedStatements[selectIndex].setInt(1, workKey);
        ResultSet rs = selectPreparedStatements[selectIndex].executeQuery();
        if (rs.next()) {
            Double value1 = rs.getDouble(1);
            selectPreparedStatements[updateIndex].setInt(1, workKey);
            rs = selectPreparedStatements[updateIndex].executeQuery();
            rs.next();
            Double value2 = rs.getDouble(1);
            updatePreparedStatements[updateIndex].setObject(1, (K - 1) * (value1 + value2) / K);
            updatePreparedStatements[updateIndex].setInt(2, workKey);
            updatePreparedStatements[updateIndex].setObject(3, (K - 1) * (value1 + value2) / K);
            updatePreparedStatements[updateIndex].execute();
        }
        mysqlConnector.commit();
    }
}
