package ecnu.db.checking;

import ecnu.db.utils.MysqlConnector;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * @author wangqingshuai
 * 工作组类，用于区别在同一组转移事务中存在的列
 */
public class WorkGroup {

    private int workId;
    private int threadsNum;
    private ArrayList<WorkNode> in = new ArrayList<>();
    private ArrayList<WorkNode> out = new ArrayList<>();
    private ArrayList<WorkNode> inout = new ArrayList<>();

    public WorkGroup(int workId) {
        this.workId = workId;
    }

    public void addInTuple(WorkNode in) {
        this.in.add(in);
    }

    public void addOutTuple(WorkNode out) {
        this.out.add(out);
    }

    public void addInoutTuple(WorkNode inout) {
        this.inout.add(inout);
    }

    public int getWorkId() {
        return workId;
    }

    @Override
    public String toString() {
        return "第" + workId + "工作组的数据为\n" + "In:" + Arrays.toString(in.toArray()) + "\n" +
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
     * 检查是否只有in或者只有out
     *
     * @return workGroup合法时返回true，否则返回false
     */
    Boolean check() {
        return !inout.isEmpty() || !in.isEmpty() && !out.isEmpty();
    }

    public void computeAllSum(boolean isBegin, MysqlConnector mysqlConnector) {
        ArrayList<WorkNode> allNode = new ArrayList<>();
        allNode.addAll(in);
        allNode.addAll(out);
        allNode.addAll(inout);
        if (isBegin) {
            for (WorkNode node : allNode) {
                node.setBeginSum(mysqlConnector.sumColumn(node.getTableIndex(), node.getTupleIndex()));
            }
        } else {
            for (WorkNode node : allNode) {
                node.setEndSum(mysqlConnector.sumColumn(node.getTableIndex(), node.getTupleIndex()));
            }
        }
    }

}
