package ecnu.db.transaction;

import ecnu.db.check.CheckNode;
import ecnu.db.schema.AbstractColumn;
import ecnu.db.schema.DecimalColumn;
import ecnu.db.utils.DatabaseConnector;
import ecnu.db.utils.ZipDistributionList;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

/**
 * @author wangqingshuai
 */
public class Remittance extends BaseTransaction {


    private final static String ADD_SQL = "update t* set tp* = tp* + ? where tp0 = ?";
    private final static String SUB_SQL = "update t* set tp* = tp* - ? where tp0 = ? and tp* > ?";
    private final static String ADD_AFTER_SELECT_SQL = "update t* set tp* = ? where tp0 = ? ";
    private final static String SUB_AFTER_SELECT_SQL = "update t* set tp* = ? where tp0 = ? and tp* > ?";
    private final static String SELECT_SQL = "select tp* from t* where tp0=?";
    private final static String SELECT_FOR_UPDATE_SQL = "select tp* from t* where tp0=? for update";
    /**
     * 事务在range范围内随机得到值时，range的大小，计算公式为
     * range= tupleRange/ rangeRandomCount
     */
    private final int rangeRandomCount;
    private final int[] range;
    private final ZipDistributionList[] addKeys;
    private final ZipDistributionList[] subKeys;
    private final String[] addSQLs;
    private final String[] subSQLs;
    private PreparedStatement[] addPrepareStatements;
    private PreparedStatement[] subPrepareStatements;
    private String[] selectSQLs;

    private PreparedStatement[] selectPrepareStatements;


    public Remittance(AbstractColumn.ColumnType columnType, ArrayList<CheckNode> checkNodes,
                      boolean isSelect, boolean forUpdate, int rangeRandomCount) {
        this.columnType = columnType;
        this.rangeRandomCount = rangeRandomCount;

        range = new int[checkNodes.size()];
        subKeys = new ZipDistributionList[checkNodes.size()];
        addKeys = new ZipDistributionList[checkNodes.size()];
        addSQLs = new String[checkNodes.size()];
        subSQLs = new String[checkNodes.size()];
        if (isSelect) {
            selectSQLs = new String[checkNodes.size()];
        }

        int i = 0;
        for (CheckNode checkNode : checkNodes) {
            if (!isSelect) {
                addSQLs[i] = ADD_SQL;
                subSQLs[i] = SUB_SQL;
            } else {
                addSQLs[i] = ADD_AFTER_SELECT_SQL;
                subSQLs[i] = SUB_AFTER_SELECT_SQL;

                if (forUpdate) {
                    selectSQLs[i] = SELECT_FOR_UPDATE_SQL;
                } else {
                    selectSQLs[i] = SELECT_SQL;
                }
                selectSQLs[i] = selectSQLs[i].replaceFirst("\\*", String.valueOf(checkNode.getColumnIndex()));
                selectSQLs[i] = selectSQLs[i].replaceFirst("\\*", String.valueOf(checkNode.getTableIndex()));
            }

            addSQLs[i] = addSQLs[i].replaceFirst("\\*", String.valueOf(checkNode.getTableIndex()));
            addSQLs[i] = addSQLs[i].replace("*", String.valueOf(checkNode.getColumnIndex()));


            subSQLs[i] = subSQLs[i].replaceFirst("\\*", String.valueOf(checkNode.getTableIndex()));
            subSQLs[i] = subSQLs[i].replace("*", String.valueOf(checkNode.getColumnIndex()));

            range[i] = checkNode.getRange();
            subKeys[i] = new ZipDistributionList(checkNode.getKeys(), true);
            addKeys[i] = new ZipDistributionList(checkNode.getKeys(), true);

            i++;
        }
    }

    @Override
    public void makePrepareStatement(DatabaseConnector databaseConnector) throws SQLException {
        this.databaseConnector = databaseConnector;
        if (selectSQLs != null) {
            selectPrepareStatements = getPreparedStatements(selectSQLs);
        }
        subPrepareStatements = getPreparedStatements(subSQLs);
        addPrepareStatements = getPreparedStatements(addSQLs);
    }


    @Override
    public void execute() throws SQLException {
        if (selectPrepareStatements == null) {
            int subIndex = R.nextInt(subPrepareStatements.length);
            Object subValue;
            if (columnType == AbstractColumn.ColumnType.DECIMAL) {
                subValue = Double.valueOf(DecimalColumn.getDf().format(R.nextDouble() * range[subIndex] / rangeRandomCount));
            } else {
                subValue = R.nextInt(range[subIndex]) / rangeRandomCount;
            }
            subPrepareStatements[subIndex].setObject(1, subValue);
            subPrepareStatements[subIndex].setInt(2, subKeys[subIndex].getValue());
            subPrepareStatements[subIndex].setObject(3, subValue);
            if (subPrepareStatements[subIndex].executeUpdate() == 1) {
                int addIndex = R.nextInt(addPrepareStatements.length);
                addPrepareStatements[addIndex].setObject(1, subValue);
                addPrepareStatements[addIndex].setInt(2, addKeys[subIndex].getValue());
                if (addPrepareStatements[addIndex].executeUpdate() == 1) {
                    databaseConnector.commit();
                    return;
                }
            }
            databaseConnector.rollback();
        } else {
            int subIndex = R.nextInt(subPrepareStatements.length);
            int subKey = subKeys[subIndex].getValue();
            selectPrepareStatements[subIndex].setInt(1, subKey);
            ResultSet rs = selectPrepareStatements[subIndex].executeQuery();
            if (rs.next()) {
                Object subValue;
                if (columnType == AbstractColumn.ColumnType.DECIMAL) {
                    subValue = Double.valueOf(DecimalColumn.getDf().format(R.nextDouble() * range[subIndex] / rangeRandomCount));
                    subPrepareStatements[subIndex].setDouble(1, rs.getDouble(1) - (double) subValue);
                    subPrepareStatements[subIndex].setDouble(3, (double) subValue);

                } else {
                    subValue = R.nextInt(range[subIndex]) / rangeRandomCount;
                    subPrepareStatements[subIndex].setInt(1, rs.getInt(1) - (int) subValue);
                    subPrepareStatements[subIndex].setInt(3, (int) subValue);
                }
                subPrepareStatements[subIndex].setInt(2, subKey);
                if (subPrepareStatements[subIndex].executeUpdate() == 1) {
                    int addIndex = R.nextInt(addPrepareStatements.length);
                    int addKey = addKeys[addIndex].getValue();
                    selectPrepareStatements[addIndex].setInt(1, addKey);
                    rs = selectPrepareStatements[addIndex].executeQuery();
                    if (rs.next()) {
                        if (columnType == AbstractColumn.ColumnType.DECIMAL) {
                            addPrepareStatements[addIndex].setDouble(1, rs.getDouble(1) + (double) subValue);
                        } else {
                            addPrepareStatements[addIndex].setInt(1, rs.getInt(1) + (int) subValue);
                        }
                        addPrepareStatements[addIndex].setInt(2, addKey);
                        if (addPrepareStatements[addIndex].executeUpdate() == 1) {
                            databaseConnector.commit();
                            return;
                        }
                    }
                }
            }
            databaseConnector.rollback();
        }
    }
}
