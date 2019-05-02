package ecnu.db.transaction;

import ecnu.db.check.WorkNode;
import ecnu.db.utils.MysqlConnector;
import ecnu.db.utils.ZipDistributionList;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

public class Order extends BaseTransaction {

    private static final String UPDATE_ORDER = "update t* set tp* = tp* - ? where tp0=? and tp* > ?";
    private static final String UPDATE_ORDER_AFTER_SELECT = "update t* set tp* = ? where tp0=? and tp* >?";

    private static final String SELECT_ORDER = "select tp* from t* where tp0=?";
    private static final String SELECT_ORDER_FOR_UPDATE = "select tp* from t* where tp0=? for update";

    private static final String INSERT_SQL = "insert into order_item values(*,*,?)";


    private String[] selectSQLS;
    private String[] updateSQLs;
    private String[] insertSQLs;

    private PreparedStatement[] selectPreparedStatements;
    private PreparedStatement[] updatePreparedStatements;
    private PreparedStatement[] insertPreparedStatements;

    private int maxOrderItemCount;
    private ZipDistributionList[] keys;

    public Order(ArrayList<WorkNode> workNodes, boolean isSelect, boolean forUpdate, int maxOrderItemCount) {
        this.maxOrderItemCount = maxOrderItemCount;

        if (isSelect) {
            selectSQLS = new String[workNodes.size()];
        }
        updateSQLs = new String[workNodes.size()];
        insertSQLs = new String[workNodes.size()];
        keys = new ZipDistributionList[workNodes.size()];
        int i = 0;
        for (WorkNode workNode : workNodes) {
            if (isSelect) {
                updateSQLs[i] = UPDATE_ORDER_AFTER_SELECT;
                if (forUpdate) {
                    selectSQLS[i] = SELECT_ORDER_FOR_UPDATE;
                } else {
                    selectSQLS[i] = SELECT_ORDER;
                }
                selectSQLS[i] = selectSQLS[i].replaceFirst("\\*", String.valueOf(workNode.getColumnIndex()));
                selectSQLS[i] = selectSQLS[i].replaceFirst("\\*", String.valueOf(workNode.getTableIndex()));

            } else {
                updateSQLs[i] = UPDATE_ORDER;
            }
            updateSQLs[i] = updateSQLs[i].replaceFirst("\\*", String.valueOf(workNode.getTableIndex()));
            updateSQLs[i] = updateSQLs[i].replace("*", String.valueOf(workNode.getColumnIndex()));

            insertSQLs[i] = INSERT_SQL;
            insertSQLs[i] = insertSQLs[i].replaceFirst("\\*", String.valueOf(workNode.getTableIndex()));
            insertSQLs[i] = insertSQLs[i].replaceFirst("\\*", String.valueOf(workNode.getColumnIndex()));

            keys[i] = new ZipDistributionList(workNodes.get(i).getKeys(), true);
            i++;
        }
    }

    @Override
    public void makePrepareStatement(MysqlConnector mysqlConnector) throws SQLException {
        this.mysqlConnector = mysqlConnector;
        if (selectSQLS != null) {
            selectPreparedStatements = getPreparedStatements(selectSQLS);
        }
        updatePreparedStatements = getPreparedStatements(updateSQLs);
        insertPreparedStatements = getPreparedStatements(insertSQLs);
    }

    @Override
    public void execute() throws SQLException {
        int updateIndex = R.nextInt(updatePreparedStatements.length);
        int orderSubCount = R.nextInt(maxOrderItemCount) + 1;
        int key = keys[updateIndex].getValue();
        if (selectPreparedStatements == null) {
            updatePreparedStatements[updateIndex].setInt(1, orderSubCount);
        } else {
            selectPreparedStatements[updateIndex].setInt(1, key);
            ResultSet rs = selectPreparedStatements[updateIndex].executeQuery();
            if (rs.next()) {
                updatePreparedStatements[updateIndex].setInt(1, rs.getInt(1) - orderSubCount);
            } else {
                mysqlConnector.rollback();
                return;
            }
        }
        updatePreparedStatements[updateIndex].setInt(2, key);
        updatePreparedStatements[updateIndex].setInt(3, orderSubCount);
        if (updatePreparedStatements[updateIndex].executeUpdate() == 1) {
            insertPreparedStatements[updateIndex].setInt(1, orderSubCount);
            if (insertPreparedStatements[updateIndex].executeUpdate() == 1) {
                mysqlConnector.commit();
                return;
            }
        }
        mysqlConnector.rollback();
    }
}
