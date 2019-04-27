package ecnu.db.check.group;

import ecnu.db.check.BaseCheckCorrectness;
import ecnu.db.check.WorkNode;
import ecnu.db.scheme.DecimalColumn;
import ecnu.db.transaction.Remittance;
import ecnu.db.utils.MysqlConnector;

import java.sql.SQLException;
import java.util.ArrayList;

public class RemittanceCheckCorrectness extends BaseCheckCorrectness {
    private final static byte SELECT_TYPE = 0b1000;
    private final static byte SELECT_FOR_UPDATE = 0b1000;

    public RemittanceCheckCorrectness() {
        super("RemittanceConfig.xml");
    }


    @Override
    public void makeTransaction() {
        transaction = new Remittance(columnType, workNodes,
                workOnTheCheckKind(SELECT_TYPE, checkKind),
                workOnTheCheckKind(SELECT_FOR_UPDATE, checkKind));
    }


    @Override
    public void computeAllSum(boolean isBegin, MysqlConnector mysqlConnector) throws SQLException {
        ArrayList<WorkNode> allNode = new ArrayList<>(workNodes);
        if (isBegin) {
            for (WorkNode node : allNode) {
                node.setBeginSum(mysqlConnector.sumColumn(node.getTableIndex(), node.getColumnIndex()));
            }
        } else {
            for (WorkNode node : allNode) {
                node.setEndSum(mysqlConnector.sumColumn(node.getTableIndex(), node.getColumnIndex()));
            }
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
