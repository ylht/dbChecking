package ecnu.db.work.group;

import ecnu.db.scheme.DoubleTuple;
import ecnu.db.utils.LoadConfig;
import ecnu.db.utils.MysqlConnector;

import java.sql.SQLException;

public class FunctionWorkGroup extends BaseWorkGroup {
    private int k;
    FunctionWorkGroup() {
        super(WorkGroupType.function);
        k= LoadConfig.getConfig().getK();
    }

    @Override
    public void computeAllSum(boolean isBegin, MysqlConnector mysqlConnector) throws SQLException {
        if (isBegin) {
            WorkNode inNode=in.get(0);
            inNode.setBeginSum(mysqlConnector.sumColumn(
                    inNode.getTableIndex(), inNode.getTupleIndex()));
            WorkNode outNode=out.get(0);
            outNode.setBeginSum(mysqlConnector.sumColumn(
                    outNode.getTableIndex(),outNode.getTupleIndex()));

        } else {
            WorkNode inNode=in.get(0);
            inNode.setEndSum(mysqlConnector.sumColumn(
                    inNode.getTableIndex(), inNode.getTupleIndex()));
            WorkNode outNode=out.get(0);
            outNode.setEndSum(mysqlConnector.sumColumn(
                    outNode.getTableIndex(),outNode.getTupleIndex()));
        }
    }

    public int getK(){
        return k;
    }

    @Override
    public boolean checkCorrect() {
        double preB = in.get(0).getBeginSum() - k * out.get(0).getBeginSum();
        double postB = in.get(0).getEndSum() - k * out.get(0).getEndSum();
        return DoubleTuple.df.format(preB).equals(DoubleTuple.df.format(postB));
    }
}
