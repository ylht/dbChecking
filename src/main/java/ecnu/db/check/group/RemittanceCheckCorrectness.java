package ecnu.db.check.group;

import ecnu.db.check.BaseCheckCorrectness;
import ecnu.db.check.WorkNode;
import ecnu.db.scheme.DecimalColumn;
import ecnu.db.transaction.Remittance;
import ecnu.db.utils.MysqlConnector;

import java.sql.SQLException;
import java.util.ArrayList;

public class RemittanceCheckCorrectness extends BaseCheckCorrectness {

    public RemittanceCheckCorrectness() {
        super("RemittanceConfig.xml");
    }


    @Override
    public void makeTransaction() {
        transaction = new Remittance(columnType, workNodes,
                checkConfigWorkOrNot("select"),
                checkConfigWorkOrNot("selectWithForUpdate"),
                config.getRangeRandomCount());
    }

    @Override
    public void recordBeginStatus(MysqlConnector mysqlConnector) throws SQLException {
        for (WorkNode node : workNodes) {
            node.setBeginSum(mysqlConnector.sumColumn(node.getTableIndex(), node.getColumnIndex()));
        }
    }

    @Override
    public void recordEndStatus(MysqlConnector mysqlConnector) throws SQLException {
        for (WorkNode node : workNodes) {
            node.setEndSum(mysqlConnector.sumColumn(node.getTableIndex(), node.getColumnIndex()));
        }
    }


    @Override
    public boolean checkCorrect() {
        Double beginSum = 0d;
        Double endSum = 0d;

        for (WorkNode node : workNodes) {
            beginSum += node.getBeginSum();
            endSum += node.getEndSum();
        }
        return DecimalColumn.getDf().format(beginSum).equals(DecimalColumn.getDf().format(endSum));
    }
}