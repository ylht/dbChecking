package ecnu.db.transaction;

import ecnu.db.check.CheckNode;
import ecnu.db.utils.DatabaseConnector;
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
    private final String[] updateSQLs;
    private final String[] insertSQLs;

    private PreparedStatement[] selectPreparedStatements;
    private PreparedStatement[] updatePreparedStatements;
    private PreparedStatement[] insertPreparedStatements;

    private final int maxOrderItemCount;
    private final ZipDistributionList[] keys;

    public Order(ArrayList<CheckNode> checkNodes, boolean isSelect, boolean forUpdate, int maxOrderItemCount) {
        this.maxOrderItemCount = maxOrderItemCount;

        if (isSelect) {
            selectSQLS = new String[checkNodes.size()];
        }
        updateSQLs = new String[checkNodes.size()];
        insertSQLs = new String[checkNodes.size()];
        keys = new ZipDistributionList[checkNodes.size()];
        int i = 0;
        for (CheckNode checkNode : checkNodes) {
            if (isSelect) {
                updateSQLs[i] = UPDATE_ORDER_AFTER_SELECT;
                if (forUpdate) {
                    selectSQLS[i] = SELECT_ORDER_FOR_UPDATE;
                } else {
                    selectSQLS[i] = SELECT_ORDER;
                }
                selectSQLS[i] = selectSQLS[i].replaceFirst("\\*", String.valueOf(checkNode.getColumnIndex()));
                selectSQLS[i] = selectSQLS[i].replaceFirst("\\*", String.valueOf(checkNode.getTableIndex()));

            } else {
                updateSQLs[i] = UPDATE_ORDER;
            }
            updateSQLs[i] = updateSQLs[i].replaceFirst("\\*", String.valueOf(checkNode.getTableIndex()));
            updateSQLs[i] = updateSQLs[i].replace("*", String.valueOf(checkNode.getColumnIndex()));

            insertSQLs[i] = INSERT_SQL;
            insertSQLs[i] = insertSQLs[i].replaceFirst("\\*", String.valueOf(checkNode.getTableIndex()));
            insertSQLs[i] = insertSQLs[i].replaceFirst("\\*", String.valueOf(checkNode.getColumnIndex()));

            keys[i] = new ZipDistributionList(checkNodes.get(i).getKeys(), true);
            i++;
        }
    }

    @Override
    public void makePrepareStatement(DatabaseConnector databaseConnector) throws SQLException {
        this.databaseConnector = databaseConnector;
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
                databaseConnector.rollback();
                return;
            }
        }
        updatePreparedStatements[updateIndex].setInt(2, key);
        updatePreparedStatements[updateIndex].setInt(3, orderSubCount);
        if (updatePreparedStatements[updateIndex].executeUpdate() == 1) {
            insertPreparedStatements[updateIndex].setInt(1, orderSubCount);
            if (insertPreparedStatements[updateIndex].executeUpdate() == 1) {
                databaseConnector.commit();
                return;
            }
        }
        databaseConnector.rollback();
    }
}
