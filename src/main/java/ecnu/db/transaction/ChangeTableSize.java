package ecnu.db.transaction;

import ecnu.db.scheme.Table;
import ecnu.db.utils.MysqlConnector;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class ChangeTableSize extends BaseTransaction {
    private boolean insert;
    private Table table;
    private int randomSize;
    private PreparedStatement changeTablePrepareStatement;


    public ChangeTableSize(boolean insert, Table table, MysqlConnector mysqlConnector)
            throws SQLException {
        super(mysqlConnector, false);
        this.insert = insert;
        this.table = table;
        if (insert) {
            changeTablePrepareStatement = mysqlConnector.getInsertStatement(
                    table.getTableIndex(), table.getTableColSizeExceptKey());
        } else {
            changeTablePrepareStatement = mysqlConnector.getDeleteStatement(
                    table.getTableIndex());
        }
    }

    @Override
    public void execute() throws SQLException {
        if (insert) {
            Object[] values = table.getInsertValue();
            int i = 1;
            for (Object value : values) {
                changeTablePrepareStatement.setObject(i++, value);
            }
            changeTablePrepareStatement.executeUpdate();
            mysqlConnector.commit();
        } else {
            changeTablePrepareStatement.setInt(1, table.getRandomKey());
            changeTablePrepareStatement.executeUpdate();
            mysqlConnector.commit();
        }
    }
}
