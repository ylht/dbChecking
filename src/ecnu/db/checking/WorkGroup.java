package ecnu.db.checking;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * @author wangqingshuai
 * 工作组类，用于区别在同一组转移事务中存在的列
 */
public class WorkGroup {

    private int workId;
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

    /**
     * 检查是否只有in或者只有out
     *
     * @return workGroup合法时返回true，否则返回false
     */
    Boolean check() {
        return !inout.isEmpty() || !in.isEmpty() && !out.isEmpty();
    }

    public ArrayList<WorkNode> getAllNode() {
        ArrayList<WorkNode> allNode = new ArrayList<>();
        allNode.addAll(in);
        allNode.addAll(out);
        allNode.addAll(inout);
        return allNode;
    }
}
