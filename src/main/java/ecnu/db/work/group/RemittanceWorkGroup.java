package ecnu.db.work.group;

import ecnu.db.scheme.DecimalColumn;
import ecnu.db.utils.MysqlConnector;

import java.sql.SQLException;
import java.util.ArrayList;

class RemittanceWorkGroup extends BaseWorkGroup {
    RemittanceWorkGroup() {
        super(WorkGroupType.remittance);
    }

    @Override
    public void computeAllSum(boolean isBegin, MysqlConnector mysqlConnector) throws SQLException {
        ArrayList<WorkNode> allNode = new ArrayList<>();
        allNode.addAll(in);
        allNode.addAll(out);
        allNode.addAll(inout);
        if (isBegin) {
            for (WorkNode node : allNode) {
                node.setBeginSum(mysqlConnector.sumColumn(node.getTableIndex(), node.getTupleIndex()));
            }
        } else {
            for (WorkNode node : allNode) {
                node.setEndSum(mysqlConnector.sumColumn(node.getTableIndex(), node.getTupleIndex()));
            }
        }
    }

    @Override
    public boolean checkCorrect() {
        Double beginSum = 0d;
        Double endSum = 0d;

        for (WorkNode node : in) {
            beginSum += node.getBeginSum();
            endSum += node.getEndSum();
        }
        for (WorkNode node : out) {
            beginSum += node.getBeginSum();
            endSum += node.getEndSum();
        }
        for (WorkNode node : inout) {
            beginSum += node.getBeginSum();
            endSum += node.getEndSum();
        }
        return DecimalColumn.df.format(beginSum).equals(DecimalColumn.df.format(endSum));
    }
}
