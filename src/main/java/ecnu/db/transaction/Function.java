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

public class Function extends BaseTransaction {

    private final static String ADD_SQL = "update t* set tp* = tp* + ? where tp0 = ?";
    private final static String ADD_AFTER_SELECT_SQL = "update t* set tp* = ? where tp0 = ? ";
    private final static String SUB_SQL = "update t* set tp* = tp* - ? where tp0 = ? and tp* > ?";
    private final static String SUB_AFTER_SELECT_SQL = "update t* set tp* = ? where tp0 = ? and tp* > ?";
    private final static String SELECT_SQL = "select tp* from t* where tp0=?";
    private final static String SELECT_FOR_UPDATE_SQL = "select tp* from t* where tp0=? for update";
    private AbstractColumn.ColumnType columnType;
    private int k;
    private boolean add;
    private int[] range;
    private ZipDistributionList[] xKeys;
    private ZipDistributionList[] yKeys;
    private String[] xSQLs;
    private PreparedStatement[] xPrepareStatements;
    private String[] ySQLs;
    private PreparedStatement[] yPrepareStatements;
    private String[] xSelectSQLs;
    private PreparedStatement[] xSelectPrepareStatements;
    private String[] ySelectSQLs;
    private PreparedStatement[] ySelectPrepareStatements;

    public Function(AbstractColumn.ColumnType columnType,
                    ArrayList<CheckNode> xNodes, ArrayList<CheckNode> yNodes,
                    boolean isSelect, boolean forUpdate, boolean add, int k) {
        this.columnType = columnType;
        this.k = k;
        this.add = add;

        if (isSelect) {
            xSelectSQLs = makeSelectInfo(forUpdate, xNodes);
            ySelectSQLs = makeSelectInfo(forUpdate, yNodes);
        }

        xSQLs = makeUpdateInfo(isSelect, add, xNodes);
        ySQLs = makeUpdateInfo(isSelect, add, yNodes);

        xKeys = makeKeysInfo(xNodes);
        yKeys = makeKeysInfo(yNodes);

        range = new int[xNodes.size()];

        for (int i = 0; i < range.length; i++) {
            range[i] = xNodes.get(i).getRange();
        }
    }

    private ZipDistributionList[] makeKeysInfo(ArrayList<CheckNode> checkNodes) {
        ZipDistributionList[] keys = new ZipDistributionList[checkNodes.size()];
        int i = 0;
        for (CheckNode checkNode : checkNodes) {
            keys[i] = new ZipDistributionList(checkNode.getKeys(), true);
            i++;
        }
        return keys;
    }


    private String[] makeSelectInfo(boolean forUpdate, ArrayList<CheckNode> checkNodes) {
        String[] selectSQLs = new String[checkNodes.size()];
        int i = 0;
        for (CheckNode checkNode : checkNodes) {
            if (forUpdate) {
                selectSQLs[i] = SELECT_FOR_UPDATE_SQL;
            } else {
                selectSQLs[i] = SELECT_SQL;
            }
            selectSQLs[i] = selectSQLs[i].replaceFirst("\\*", String.valueOf(checkNode.getColumnIndex()));
            selectSQLs[i] = selectSQLs[i].replaceFirst("\\*", String.valueOf(checkNode.getTableIndex()));
            i++;
        }
        return selectSQLs;
    }


    private String[] makeUpdateInfo(boolean isSelect, boolean add, ArrayList<CheckNode> checkNodes) {
        String[] updateSQLs = new String[checkNodes.size()];
        int i = 0;
        for (CheckNode checkNode : checkNodes) {
            if (isSelect) {
                if (add) {
                    updateSQLs[i] = ADD_AFTER_SELECT_SQL;
                } else {
                    updateSQLs[i] = SUB_AFTER_SELECT_SQL;
                }
            } else {
                if (add) {
                    updateSQLs[i] = ADD_SQL;
                } else {
                    updateSQLs[i] = SUB_SQL;
                }
            }

            updateSQLs[i] = updateSQLs[i].replaceFirst("\\*", String.valueOf(checkNode.getTableIndex()));
            updateSQLs[i] = updateSQLs[i].replace("*", String.valueOf(checkNode.getColumnIndex()));
            i++;
        }
        return updateSQLs;
    }


    @Override
    public void makePrepareStatement(DatabaseConnector databaseConnector) throws SQLException {
        this.databaseConnector = databaseConnector;
        if (xSelectSQLs != null) {
            xSelectPrepareStatements = getPreparedStatements(xSelectSQLs);
            ySelectPrepareStatements = getPreparedStatements(ySelectSQLs);
        }
        xPrepareStatements = getPreparedStatements(xSQLs);
        yPrepareStatements = getPreparedStatements(ySQLs);
    }


    private void executeUpdateTransaction() throws SQLException {
        int xIndex = R.nextInt(xPrepareStatements.length);
        int yIndex = R.nextInt(yPrepareStatements.length);
        Object xValue;
        if (columnType == AbstractColumn.ColumnType.DECIMAL) {
            xValue = Double.valueOf(DecimalColumn.getDf().format(R.nextDouble() * range[xIndex]));
        } else {
            xValue = R.nextInt(range[xIndex]);
        }
        xPrepareStatements[xIndex].setObject(1, xValue);
        xPrepareStatements[xIndex].setInt(2, xKeys[xIndex].getValue());
        if (!add) {
            xPrepareStatements[xIndex].setObject(3, xValue);
        }
        if (xPrepareStatements[xIndex].executeUpdate() == 1) {

            if (columnType == AbstractColumn.ColumnType.DECIMAL) {
                yPrepareStatements[yIndex].setDouble(1, k * (double) xValue);
                if (!add) {
                    yPrepareStatements[yIndex].setDouble(3, k * (double) xValue);
                }
            } else {
                yPrepareStatements[yIndex].setInt(1, k * (int) xValue);
                if (!add) {
                    yPrepareStatements[yIndex].setInt(3, k * (int) xValue);
                }
            }
            yPrepareStatements[yIndex].setInt(2, yKeys[yIndex].getValue());
            if (yPrepareStatements[yIndex].executeUpdate() == 1) {
                databaseConnector.commit();
                return;
            }
        }
        databaseConnector.rollback();
    }

    private void executeTransactionWithSelect() throws SQLException {
        int xIndex = R.nextInt(xPrepareStatements.length);
        int xKey = xKeys[xIndex].getValue();
        xSelectPrepareStatements[xIndex].setInt(1, xKey);
        ResultSet rs = xSelectPrepareStatements[xIndex].executeQuery();
        if (rs.next()) {
            Object xValue;
            if (columnType == AbstractColumn.ColumnType.DECIMAL) {
                xValue = Double.valueOf(DecimalColumn.getDf().format(R.nextDouble() * range[xIndex]));
                if (add) {
                    xPrepareStatements[xIndex].setDouble(1, rs.getDouble(1) + (double) xValue);
                } else {
                    xPrepareStatements[xIndex].setDouble(1, rs.getDouble(1) - (double) xValue);
                    xPrepareStatements[xIndex].setDouble(3, (double) xValue);
                }
            } else {
                xValue = R.nextInt(range[xIndex]) + 1;
                if (add) {
                    xPrepareStatements[xIndex].setInt(1, rs.getInt(1) + (int) xValue);
                } else {
                    xPrepareStatements[xIndex].setInt(1, rs.getInt(1) - (int) xValue);
                    xPrepareStatements[xIndex].setInt(3, (int) xValue);
                }
            }

            xPrepareStatements[xIndex].setInt(2, xKey);
            if (xPrepareStatements[xIndex].executeUpdate() == 1) {
                int yIndex = R.nextInt(yPrepareStatements.length);
                int yKey = yKeys[yIndex].getValue();
                ySelectPrepareStatements[yIndex].setInt(1, yKey);
                rs = ySelectPrepareStatements[yIndex].executeQuery();
                if (rs.next()) {
                    if (columnType == AbstractColumn.ColumnType.DECIMAL) {
                        if (add) {
                            yPrepareStatements[yIndex].setDouble(1, rs.getDouble(1) + k * (double) xValue);
                        } else {
                            yPrepareStatements[yIndex].setDouble(1, rs.getDouble(1) - k * (double) xValue);
                            yPrepareStatements[yIndex].setDouble(3, k * (double) xValue);
                        }
                    } else {
                        if (add) {
                            yPrepareStatements[yIndex].setInt(1, rs.getInt(1) + k * (int) xValue);
                        } else {
                            yPrepareStatements[yIndex].setInt(1, rs.getInt(1) - k * (int) xValue);
                            yPrepareStatements[yIndex].setInt(3, k * (int) xValue);
                        }
                    }
                    yPrepareStatements[yIndex].setInt(2, yKey);
                    if (yPrepareStatements[yIndex].executeUpdate() == 1) {
                        databaseConnector.commit();

                        return;
                    }
                }
            }
            databaseConnector.rollback();
        }
    }


    @Override
    public void execute() throws SQLException {
        if (xSelectPrepareStatements == null) {
            executeUpdateTransaction();
        } else {
            executeTransactionWithSelect();
        }
    }
}
