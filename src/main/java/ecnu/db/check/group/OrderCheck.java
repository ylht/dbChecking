package ecnu.db.check.group;

import ecnu.db.check.BaseCheck;
import ecnu.db.check.WorkNode;
import ecnu.db.transaction.Order;
import ecnu.db.utils.MysqlConnector;

import java.sql.SQLException;

public class OrderCheck extends BaseCheck {
    public OrderCheck() {
        super("OrderConfig.xml");
    }

    @Override
    public void makeTransaction() {
        transaction = new Order(workNodes, checkConfigWorkOrNot("select"),
                checkConfigWorkOrNot("selectWithForUpdate"), config.getOrderMaxCount());
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
