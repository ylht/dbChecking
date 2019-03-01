package ecnu.db.transaction;

import ecnu.db.core.WorkGroup;
import ecnu.db.core.WorkNode;
import ecnu.db.scheme.Table;
import ecnu.db.utils.MysqlConnector;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

/**
 * @author wangqingshuai
 * 处理order类型事务的线程
 */
public class OrderTransaction extends BaseTransaction {


    private Table[] tables;
    private ArrayList<WorkNode> nodes = new ArrayList<>();

    private ArrayList<PreparedStatement> subStatement = new ArrayList<>();
    private ArrayList<PreparedStatement> subSelectStatement = new ArrayList<>();

    public OrderTransaction(Table[] tables, WorkGroup workGroup, MysqlConnector mysqlConnector) throws SQLException {
        super(mysqlConnector, false);
        orderTransaction(tables, workGroup, mysqlConnector);
    }

    public OrderTransaction(Table[] tables, WorkGroup workGroup,
                            MysqlConnector mysqlConnector, boolean forUpdate) throws SQLException {
        super(mysqlConnector, true);
        orderTransaction(tables, workGroup, mysqlConnector);
        for (WorkNode node : nodes) {
            subSelectStatement.add(mysqlConnector.getSelect(forUpdate, node.getTableIndex(),
                    node.getTupleIndex()));
        }

    }

    private void orderTransaction(Table[] tables, WorkGroup workGroup,
                                  MysqlConnector mysqlConnector) throws SQLException {
        //确保工作组的类型正确
        assert workGroup.getWorkGroupType() == WorkGroup.WorkGroupType.order;
        this.tables = tables;
        nodes.addAll(workGroup.getIn());
        for (WorkNode node : nodes) {
            subStatement.add(mysqlConnector.getOrderUpdate(node.getTableIndex(),
                    node.getTupleIndex(), isSelect));
        }
    }

    @Override
    public void execute() throws SQLException {
        int randomInIndex = r.nextInt(nodes.size());
        preparedOutStatement = subStatement.get(randomInIndex);
        if (isSelect) {
            preparedOutSelectStatement = subSelectStatement.get(randomInIndex);
        }
        WorkNode work = nodes.get(randomInIndex);


        int workPriKey = work.getSubValueList().get(
                tables[work.getTableIndex()].getDistributionIndex() - 1);
        if (isSelect) {
            preparedOutSelectStatement.setInt(1, workPriKey);
            ResultSet rs = preparedOutSelectStatement.executeQuery();
            rs.next();
            preparedOutStatement.setInt(1, rs.getInt(1) - 1);
            preparedOutStatement.setInt(2, workPriKey);
        } else {
            preparedOutStatement.setInt(1, workPriKey);
        }
        if (preparedOutStatement.executeUpdate() == 0) {
            System.out.println("执行失败" + preparedOutStatement.toString());
            mysqlConnector.rollback();
            return;
        }
        mysqlConnector.commit();
    }
}
