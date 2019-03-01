package ecnu.db.core;

import ecnu.db.scheme.DoubleTuple;
import ecnu.db.utils.MysqlConnector;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * @author wangqingshuai
 * 工作组类，用于区别在同一组事务中存在的列
 * <p>
 * 事务的操作主要有三种
 * 1. 转账事务 在不同列之间互相转移数据
 * 2. y=kx+b事务，在两个列之间维持该关系式
 * 3. 订单事务，在单列上的tuple上累加，并写入日志，判定日志与服务器的数据是否一致。
 */
public class WorkGroup {


    private WorkGroupType workGroupType;
    private int k;
    private ArrayList<WorkNode> in = new ArrayList<>();
    private ArrayList<WorkNode> out = new ArrayList<>();
    private ArrayList<WorkNode> inout = new ArrayList<>();
    public WorkGroup(WorkGroupType workGroupType) {
        this.workGroupType = workGroupType;
    }

    public WorkGroupType getWorkGroupType() {
        return workGroupType;
    }

    void addInTuple(WorkNode in) {
        this.in.add(in);
    }

    void addOutTuple(WorkNode out) {
        this.out.add(out);
    }

    void addInoutTuple(WorkNode inout) {
        this.inout.add(inout);
    }

    @Override
    public String toString() {
        return "工作组的类型为" + workGroupType + ",数据为\n" + "In:" + Arrays.toString(in.toArray()) + "\n" +
                "Out:" + Arrays.toString(out.toArray()) + "\n" + "Inout:" + Arrays.toString(inout.toArray());
    }

    public ArrayList<WorkNode> getIn() {
        ArrayList<WorkNode> result = new ArrayList<>(in);
        result.addAll(inout);
        return result;
    }

    public ArrayList<WorkNode> getOut() {
        ArrayList<WorkNode> result = new ArrayList<>(out);
        result.addAll(inout);
        return result;
    }

    void computeAllSum(boolean isBegin, MysqlConnector mysqlConnector) throws SQLException {
        ArrayList<WorkNode> allNode = new ArrayList<>();
        allNode.addAll(in);
        allNode.addAll(out);
        allNode.addAll(inout);

        if (isBegin) {
            for (WorkNode node : allNode) {
                node.setBeginSum(mysqlConnector.sumColumn(node.getTableIndex(), node.getTupleIndex()));
            }
            if (workGroupType == WorkGroupType.function) {
                k = (int) (in.get(0).getBeginSum() / out.get(0).getBeginSum());
            }
        } else {
            if (workGroupType == WorkGroupType.writeSkew) {
                int errCount = mysqlConnector.getWriteSkewResult(allNode);
                for (WorkNode node : allNode) {
                    node.setEndSum((double) errCount);
                }
            } else {
                for (WorkNode node : allNode) {
                    node.setEndSum(mysqlConnector.sumColumn(node.getTableIndex(), node.getTupleIndex()));
                }
            }

        }

    }

    public int getK() {
        return k;
    }

    private boolean remittanceCheck() {
        Double beginSum = 0d;
        Double endSum = 0d;

        for (WorkNode node : in) {
            beginSum += node.getBeginSum();
            endSum += node.getEndSum();
        }
        for (WorkNode node : out) {
            beginSum += node.getBeginSum();
            endSum += node.getEndSum();
        }
        for (WorkNode node : inout) {
            beginSum += node.getBeginSum();
            endSum += node.getEndSum();
        }
        return DoubleTuple.df.format(beginSum).equals(DoubleTuple.df.format(endSum));
    }

    private boolean functionCheck() {
        double preB = in.get(0).getBeginSum() - k * out.get(0).getBeginSum();
        double postB = in.get(0).getEndSum() - k * out.get(0).getEndSum();
        return DoubleTuple.df.format(preB).equals(DoubleTuple.df.format(postB));
    }

    boolean checkCorrect() {
        switch (workGroupType) {
            case remittance:
                return remittanceCheck();
            case function:
                return functionCheck();
            case order:
                return orderCheck();
            case writeSkew:
                return writeSkewCheck();
            default:
                return false;
        }
    }

    private boolean writeSkewCheck() {
        return out.get(0).getEndSum() == 0;
    }

    private boolean orderCheck() {
        int total = in.size();

        return true;
    }

    public enum WorkGroupType {
        /*转账*/
        remittance,
        /*kx+b的函数式关系*/
        function,
        /*order关系*/
        order,
        /*writeSkew业务*/
        writeSkew
    }
}
