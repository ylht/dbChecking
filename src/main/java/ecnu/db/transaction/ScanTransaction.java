package ecnu.db.transaction;

import ecnu.db.scheme.Table;
import ecnu.db.utils.MysqlConnector;
import ecnu.db.work.group.BaseWorkGroup;
import ecnu.db.work.group.WorkNode;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;

public class ScanTransaction extends BaseTransaction {
    private Table table;
    private int tupleIndex;

    public ScanTransaction(Table[] tables, BaseWorkGroup workGroup,
                           MysqlConnector mysqlConnector) throws SQLException {
        super(mysqlConnector, false);
        WorkNode node = workGroup.getIn().get(0);
        table = tables[node.getTableIndex()];
        this.tupleIndex = node.getTupleIndex();
        preparedInSelectStatement = mysqlConnector.getScanStatement(
                node.getTableIndex(), node.getTupleIndex());
        preparedInStatement = mysqlConnector.getUpdateAllStatement(
                node.getTableIndex(), node.getTupleIndex());
        preparedOutStatement = mysqlConnector.getInsertPhantomReadRecordStatement(
                node.getTableIndex());
    }

    private ArrayList<Integer> datas(double max, double min) throws SQLException {
        if (max < min) {
            max = max + min;
            min = max - min;
            max = max - min;
        }
        preparedInSelectStatement.setDouble(1, min);
        preparedInSelectStatement.setDouble(2, max);
        ResultSet rs = preparedInSelectStatement.executeQuery();
        ArrayList<Integer> datas = new ArrayList<>();

        while (rs.next()) {
            datas.add(rs.getInt(1));
        }
        return datas;
    }

    @Override
    public void execute() throws SQLException {
        if(r.nextDouble()<0.9){
            return;
        }
        double min = table.getRandomValue(tupleIndex);
        double max = table.getRandomValue(tupleIndex);
        ArrayList<Integer> oldData = datas(max, min);
        //保证间隔 使其有可变更数据的空间
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        //整体数据加1，以验证mysql版本的幻读
        preparedInStatement.setObject(1, min);
        preparedInStatement.setObject(2, max);
        preparedInStatement.executeUpdate();
        ArrayList<Integer> newData = datas(max + 1, min + 1);
        mysqlConnector.rollback();
        //开始在本地验证结果集的正确性
        if (oldData.size() == newData.size()) {
            Collections.sort(oldData);
            Collections.sort(newData);
            for (int j = 0; j < oldData.size(); j++) {
                if (!oldData.get(j).equals(newData.get(j))) {
                    preparedOutStatement.setInt(1, 1);
                    preparedOutStatement.executeUpdate();
                    break;
                }
            }
        } else {
            preparedOutStatement.setInt(1, -1);
            preparedOutStatement.executeUpdate();
        }
        mysqlConnector.commit();

    }
}




