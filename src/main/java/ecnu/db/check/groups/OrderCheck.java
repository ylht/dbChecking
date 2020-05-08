package ecnu.db.check.groups;

import ecnu.db.check.BaseCheck;
import ecnu.db.check.CheckNode;
import ecnu.db.transaction.Order;
import ecnu.db.utils.DatabaseConnector;

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
    public void recordBeginStatus(DatabaseConnector databaseConnector) throws SQLException {
        for (CheckNode node : checkNodes) {
            node.setBeginSum(databaseConnector.sumColumn(node.getTableIndex(), node.getColumnIndex()));
        }
    }

    @Override
    public void recordEndStatus(DatabaseConnector databaseConnector) throws SQLException {


        for (CheckNode node : checkNodes) {
            node.setEndSum(databaseConnector.sumColumn(node.getTableIndex(), node.getColumnIndex()));

            String sql = "select sum(num) from order_item" +
                    " where tableIndex=" + node.getTableIndex() + " and tupleIndex =" + node.getColumnIndex();

            node.setOrderNum(databaseConnector.getResult(sql));
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
