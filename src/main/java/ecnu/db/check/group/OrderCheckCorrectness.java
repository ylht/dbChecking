package ecnu.db.check.group;

import ecnu.db.check.BaseCheckCorrectness;
import ecnu.db.check.WorkNode;
import ecnu.db.utils.MysqlConnector;

import java.sql.SQLException;

public class OrderCheckCorrectness extends BaseCheckCorrectness {
    public OrderCheckCorrectness() {
        super("SampleConfig.xml");
    }

    @Override
    public void makeTransaction() {

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
            node.setOrderNum(mysqlConnector.getOrderItem(node.getTableIndex(), node.getColumnIndex()));
        }
    }


    @Override
    public boolean checkCorrect() {
        for (WorkNode node : workNodes) {
            if (node.getOrderNum() != node.getBeginSum() - node.getEndSum()) {
                return false;
            }
        }
        return true;
    }
}
