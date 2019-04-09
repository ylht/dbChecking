package ecnu.db.transaction;

import ecnu.db.scheme.Table;
import ecnu.db.utils.MysqlConnector;
import ecnu.db.work.group.BaseWorkGroup;
import ecnu.db.work.group.WorkNode;

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

    private ArrayList<PreparedStatement> subStatements = new ArrayList<>();
    private ArrayList<PreparedStatement> subSelectStatements = new ArrayList<>();
    private ArrayList<PreparedStatement> insertStatements = new ArrayList<>();

    public OrderTransaction(Table[] tables, BaseWorkGroup workGroup, MysqlConnector mysqlConnector) throws SQLException {
        super(mysqlConnector, false);
        orderTransaction(tables, workGroup, mysqlConnector);
    }

    public OrderTransaction(Table[] tables, BaseWorkGroup workGroup,
                            MysqlConnector mysqlConnector, boolean forUpdate) throws SQLException {
        super(mysqlConnector, true);
        orderTransaction(tables, workGroup, mysqlConnector);
        for (WorkNode node : nodes) {
            subSelectStatements.add(mysqlConnector.getSelect(forUpdate, node.getTableIndex(),
                    node.getTupleIndex()));
        }

    }

    private void orderTransaction(Table[] tables, BaseWorkGroup workGroup,
                                  MysqlConnector mysqlConnector) throws SQLException {
        //确保工作组的类型正确
        assert workGroup.getWorkGroupType() == BaseWorkGroup.WorkGroupType.order;
        this.tables = tables;
        nodes.addAll(workGroup.getIn());
        for (WorkNode node : nodes) {
            subStatements.add(mysqlConnector.getOrderUpdate(node.getTableIndex(),
                    node.getTupleIndex(), isSelect));
            insertStatements.add(mysqlConnector.insertOrderItem(
                    node.getTableIndex(), node.getTupleIndex()));
        }
    }

    @Override
    public void execute() throws SQLException {
        int randomOutIndex = r.nextInt(nodes.size());
        preparedOutStatement = subStatements.get(randomOutIndex);
        if (isSelect) {
            preparedOutSelectStatement = subSelectStatements.get(randomOutIndex);
        }
        WorkNode work = nodes.get(randomOutIndex);
        int workPriKey = work.getSubKey();
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
        } else {
            insertStatements.get(randomOutIndex).execute();
        }
        mysqlConnector.commit();
    }
}
