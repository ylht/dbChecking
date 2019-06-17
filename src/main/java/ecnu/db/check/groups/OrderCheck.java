package ecnu.db.check.groups;

import ecnu.db.check.BaseCheck;
import ecnu.db.check.CheckNode;
import ecnu.db.transaction.Order;
import ecnu.db.utils.MysqlConnector;

import java.sql.SQLException;

public class OrderCheck extends BaseCheck {
    public OrderCheck() {
        super("OrderConfig.xml");
    }

    @Override
    public void makeTransaction() {
        transaction = new Order(checkNodes, checkConfigWorkOrNot("select"),
                checkConfigWorkOrNot("selectWithForUpdate"), config.getOrderMaxCount());
    }

    @Override
    public void recordBeginStatus(MysqlConnector mysqlConnector) throws SQLException {
        for (CheckNode node : checkNodes) {
            node.setBeginSum(mysqlConnector.sumColumn(node.getTableIndex(), node.getColumnIndex()));
        }
    }

    @Override
    public void recordEndStatus(MysqlConnector mysqlConnector) throws SQLException {


        for (CheckNode node : checkNodes) {
            node.setEndSum(mysqlConnector.sumColumn(node.getTableIndex(), node.getColumnIndex()));

            String sql = "select sum(num) from order_item" +
                    " where tableIndex=" + node.getTableIndex() + " and tupleIndex =" + node.getColumnIndex();

            node.setOrderNum(mysqlConnector.getResult(sql));
        }
    }


    @Override
    public boolean checkCorrect() {
        for (CheckNode node : checkNodes) {
            if (node.getOrderNum() != node.getBeginSum() - node.getEndSum()) {
                return false;
            }
        }
        return true;
    }
}
