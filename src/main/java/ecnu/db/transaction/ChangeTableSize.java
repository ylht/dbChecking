package ecnu.db.transaction;

import ecnu.db.scheme.Table;
import ecnu.db.utils.MysqlConnector;
import org.apache.logging.log4j.LogManager;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class ChangeTableSize extends BaseTransaction {
    private boolean insert;
    private Table table;
    private int randomSize;
    private PreparedStatement changeTablePrepareStatement;


    public ChangeTableSize(boolean insert, Table table, MysqlConnector mysqlConnector) {
        super(mysqlConnector, false);
        this.insert = insert;
        this.table = table;
        this.conn = mysqlConnector.getConn();
        if (insert) {
            changeTablePrepareStatement = mysqlConnector.getInsertStatement(
                    table.getTableIndex(), table.getTableColSizeExceptKey());
        } else {
            changeTablePrepareStatement = mysqlConnector.getDeleteStatement(
                    table.getTableIndex());
        }
    }

    @Override
    public void execute() {
        if (insert) {
            Object[] values = table.getInsertValue();
            try {
                int i = 1;
                for (Object value : values) {
                    changeTablePrepareStatement.setObject(i++, value);
                }
                changeTablePrepareStatement.executeUpdate();
                conn.commit();
            } catch (SQLException e) {
                System.out.println(changeTablePrepareStatement);
                LogManager.getLogger().error(e);
            }
        } else {
            try {
                changeTablePrepareStatement.setInt(1, table.getRandomKey());
                changeTablePrepareStatement.executeUpdate();
                conn.commit();
            } catch (SQLException e) {
                System.out.println(changeTablePrepareStatement);
                LogManager.getLogger().error(e);
            }

        }
    }
}
