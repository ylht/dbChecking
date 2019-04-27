package ecnu.db.transaction;

import ecnu.db.check.WorkNode;
import ecnu.db.scheme.AbstractColumn;
import ecnu.db.utils.MysqlConnector;
import ecnu.db.utils.ZipDistributionList;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;

public class Function extends BaseTransaction {

    private final static String ADD_SQL = "update t* set tp* = tp* + ? where tp0 = ?";
    private final static String ADD_AFTER_SELECT_SQL = "update t* set tp* = ? where tp0 = ? ";
    private final static String SUB_SQL = "update t* set tp* = tp* - ? where tp0 = ? and tp* > ?";
    private final static String SUB_AFTER_SELECT_SQL = "update t* set tp* = ? where tp0 = ? and tp* > ?";
    private final static String SELECT_SQL = "select tp* from t * where tp0=?";
    private final static String SELECT_FOR_UPDATE_SQL = "select tp* from t * where tp0=? for update";
    private AbstractColumn.ColumnType columnType;
    private int[] range;
    private ZipDistributionList[] addKeys;
    private ZipDistributionList[] subKeys;
    private String[] addSQLs;
    private PreparedStatement[] addPrepareStatements;
    private String[] subSQLs;
    private PreparedStatement[] subPrepareStatements;
    private String[] selectSQLs;
    private PreparedStatement[] selectPrepareStatements;

    public Function(AbstractColumn.ColumnType columnType, ArrayList<WorkNode> workNodes,
                    boolean isSelect, boolean forUpdate) {

    }


    @Override
    public void makePrepareStatement(MysqlConnector mysqlConnector) throws SQLException {

    }

    @Override
    public void execute() throws SQLException {

    }
}
