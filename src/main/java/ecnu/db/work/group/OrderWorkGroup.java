package ecnu.db.work.group;

import ecnu.db.utils.MysqlConnector;

import java.sql.SQLException;

class OrderWorkGroup extends BaseWorkGroup {
    OrderWorkGroup() {
        super(WorkGroupType.order);
    }

    @Override
    public void computeAllSum(boolean isBegin, MysqlConnector mysqlConnector) throws SQLException {
        if (isBegin) {
            for (WorkNode node : in) {
                node.setBeginSum(mysqlConnector.sumColumn(node.getTableIndex(), node.getTupleIndex()));
            }
        } else {
            for (WorkNode node : in) {
                node.setEndSum(mysqlConnector.sumColumn(node.getTableIndex(), node.getTupleIndex()));
                node.setOrderNum(mysqlConnector.getOrderItem(node.getTableIndex(), node.getTupleIndex()));
            }
        }
    }

    @Override
    public boolean checkCorrect() {
        for(WorkNode node:in){
            if(node.getOrderNum()!=node.getBeginSum()-node.getEndSum()){
                return false;
            }
        }
        return true;
    }
}
