package ecnu.db.check.group;

import ecnu.db.check.BaseCheckCorrectness;
import ecnu.db.check.WorkNode;
import ecnu.db.scheme.DecimalColumn;
import ecnu.db.utils.MysqlConnector;

import java.sql.SQLException;

public class FunctionCheckCorrectness extends BaseCheckCorrectness {
    private final static int COLUMN_COUNT = 2;
    private int k;

    public FunctionCheckCorrectness() {
        super("SampleConfig.xml");
    }


    @Override
    public boolean workOnWorked() {
        return false;
    }

    @Override
    public void makeTransaction() {

    }

    @Override
    public int getColumnCount() {
        return COLUMN_COUNT;
    }

    @Override
    public void computeAllSum(boolean isBegin, MysqlConnector mysqlConnector) throws SQLException {
        if (isBegin) {
            WorkNode inNode = workNodes.get(0);
            inNode.setBeginSum(mysqlConnector.sumColumn(
                    inNode.getTableIndex(), inNode.getColumnIndex()));
            WorkNode outNode = workNodes.get(1);
            outNode.setBeginSum(mysqlConnector.sumColumn(
                    outNode.getTableIndex(), outNode.getColumnIndex()));

        } else {
            WorkNode inNode = workNodes.get(0);
            inNode.setEndSum(mysqlConnector.sumColumn(
                    inNode.getTableIndex(), inNode.getColumnIndex()));
            WorkNode outNode = workNodes.get(1);
            outNode.setEndSum(mysqlConnector.sumColumn(
                    outNode.getTableIndex(), outNode.getColumnIndex()));
        }
    }

    public int getK() {
        return k;
    }

    @Override
    public boolean checkCorrect() {
        double preB = workNodes.get(0).getBeginSum() - k * workNodes.get(1).getBeginSum();
        double postB = workNodes.get(0).getEndSum() - k * workNodes.get(1).getEndSum();
        return DecimalColumn.getDf().format(preB).equals(DecimalColumn.getDf().format(postB));
    }
}
