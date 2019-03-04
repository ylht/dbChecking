package ecnu.db.transaction;

import ecnu.db.scheme.Table;
import ecnu.db.utils.LoadConfig;
import ecnu.db.utils.MysqlConnector;
import ecnu.db.work.group.BaseWorkGroup;
import ecnu.db.work.group.WorkNode;

import java.sql.SQLException;

public class ChangeTableSize extends BaseTransaction {
    private Table table;

    public ChangeTableSize(Table[] tables, BaseWorkGroup workGroup, MysqlConnector mysqlConnector)
            throws SQLException {
        super(mysqlConnector, false);
        WorkNode node=workGroup.getIn().get(0);
        table=tables[node.getTableIndex()];
        preparedInStatement = mysqlConnector.getInsertStatement(
                node.getTableIndex(), tables[node.getTableIndex()].getTableColSizeForInsert());
        preparedOutStatement = mysqlConnector.getDeleteStatement(node.getTableIndex());

    }

    @Override
    public void execute() throws SQLException {
        if (r.nextDouble()> LoadConfig.getConfig().getTableSparsity()) {
            Object[] values = table.getInsertValue();
            int i = 1;
            for (Object value : values) {
                preparedInStatement.setObject(i++, value);
            }
            preparedInStatement.executeUpdate();
            mysqlConnector.commit();
        } else {
            preparedOutStatement.setInt(1, table.getRandomKey());
            preparedOutStatement.executeUpdate();
            mysqlConnector.commit();
        }
    }
}
