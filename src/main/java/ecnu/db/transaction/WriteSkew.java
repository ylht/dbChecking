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

    private static final String SELECT_SQL = "select tp* , tp* from t* where tp0 =?";
    private static final String UPDATE_SQL = "update t* set tp* = tp* - ? where tp0 =?";

    private String selectSQL;
    private String[] updateSQLs;

    private PreparedStatement selectPreparedStatement;
    private PreparedStatement[] updatePreparedStatements;

    private ZipDistributionList key;

    public WriteSkew(ArrayList<WorkNode> workNodes) {
        assert workNodes.size() == 2;
        assert workNodes.get(0).getTableIndex() == workNodes.get(1).getTableIndex();

        selectSQL = SELECT_SQL;
        for (WorkNode workNode : workNodes) {
            selectSQL = selectSQL.replaceFirst("\\*", String.valueOf(workNode.getColumnIndex()));
        }
        selectSQL = selectSQL.replaceFirst("\\*", String.valueOf(workNodes.get(0).getTableIndex()));
        updateSQLs = new String[workNodes.size()];
        for (int i = 0; i < workNodes.size(); i++) {
            updateSQLs[i] = UPDATE_SQL;
            updateSQLs[i] = updateSQLs[i].replaceFirst("\\*", String.valueOf(workNodes.get(i).getTableIndex()));
            updateSQLs[i] = updateSQLs[i].replace("*", String.valueOf(workNodes.get(i).getColumnIndex()));
        }
        key = new ZipDistributionList(workNodes.get(0).getKeys(), true);
    }

    @Override
    public void makePrepareStatement(MysqlConnector mysqlConnector) throws SQLException {
        this.mysqlConnector = mysqlConnector;
        selectPreparedStatement = mysqlConnector.getPrepareStatement(selectSQL);
        updatePreparedStatements = getPreparedStatements(updateSQLs);
    }

    @Override
    public void execute() throws SQLException {
        int workKey = key.getValue();
        int updateIndex = R.nextInt(updatePreparedStatements.length);
        selectPreparedStatement.setInt(1, workKey);
        ResultSet rs = selectPreparedStatement.executeQuery();
        if (rs.next()) {
            Double value1 = rs.getDouble(1);
            Double value2 = rs.getDouble(2);
            if (value1 + value2 > 0) {
                updatePreparedStatements[updateIndex].setObject(1, (K - 1) * (value1 + value2) / K);
                updatePreparedStatements[updateIndex].setInt(2, workKey);
                updatePreparedStatements[updateIndex].execute();
            }
        }
        mysqlConnector.commit();
    }
}
