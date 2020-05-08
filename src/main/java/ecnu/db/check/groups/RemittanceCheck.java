package ecnu.db.check.groups;

import ecnu.db.check.BaseCheck;
import ecnu.db.check.CheckNode;
import ecnu.db.transaction.Remittance;
import ecnu.db.utils.DatabaseConnector;

import java.math.BigDecimal;
import java.sql.SQLException;

public class RemittanceCheck extends BaseCheck {

    public RemittanceCheck() {
        super("RemittanceConfig.xml");
    }


    @Override
    public void makeTransaction() {
        transaction = new Remittance(columnType, checkNodes,
                checkConfigWorkOrNot("select"),
                checkConfigWorkOrNot("selectWithForUpdate"),
                config.getRangeRandomCount());
    }

    @Override
    public void recordBeginStatus(DatabaseConnector databaseConnector) throws SQLException {
        for (CheckNode node : checkNodes) {
            node.setBeginSum(databaseConnector.sumColumn(node.getTableIndex(), node.getColumnIndex()));
        }
    }

    @Override
    public void recordEndStatus(DatabaseConnector databaseConnector) throws SQLException {
        for (CheckNode node : checkNodes) {
            node.setEndSum(databaseConnector.sumColumn(node.getTableIndex(), node.getColumnIndex()));
        }
    }


    @Override
    public boolean checkCorrect() {
        BigDecimal beginSum = new BigDecimal(0);
        BigDecimal endSum = new BigDecimal(0);

        for (CheckNode node : checkNodes) {
            beginSum = beginSum.add(BigDecimal.valueOf(node.getBeginSum()));
            endSum = endSum.add(BigDecimal.valueOf(node.getEndSum()));
        }
        return beginSum.equals(endSum);
    }
}
