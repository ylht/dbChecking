package ecnu.db.work.group;

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
public abstract class BaseWorkGroup {


    private WorkGroupType workGroupType;
    ArrayList<WorkNode> in = new ArrayList<>();
    ArrayList<WorkNode> out = new ArrayList<>();
    ArrayList<WorkNode> inout = new ArrayList<>();
    BaseWorkGroup(WorkGroupType workGroupType) {
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

    /**
     * 计算用于验证工作组正确性的相关数据
     * @param isBegin 是否计算初始状态
     * @param mysqlConnector 加载数据库驱动
     * @throws SQLException 抛出异常
     */
    public abstract void computeAllSum(boolean isBegin, MysqlConnector mysqlConnector) throws SQLException;

    /**
     * 该工作组是否维持了一致性
     * @return 满足返回true否则返回false
     */
    public abstract boolean checkCorrect();

    public enum WorkGroupType {
        /*转账*/
        remittance,
        /*kx+b的函数式关系*/
        function,
        /*order关系*/
        order,
        /*writeSkew业务*/
        writeSkew,
        /*检测是否存在未提交的数据*/
        noCommit,
        /*检测两次读到的数据是否相同*/
        repeatableRead,
        /*检测是否出现了幻读*/
        phantomRead
    }
}
