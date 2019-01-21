package ecnu.db.checking;

import ecnu.db.scheme.DoubleTuple;
import ecnu.db.utils.MysqlConnector;

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

    private int workId;
    private WorkGroupType workGroupType;

    private int k;
    private ArrayList<WorkNode> in = new ArrayList<>();
    private ArrayList<WorkNode> out = new ArrayList<>();
    private ArrayList<WorkNode> inout = new ArrayList<>();
    public WorkGroup(int workId) {
        this.workId = workId;
    }

    public WorkGroupType getWorkGroupType() {
        return workGroupType;
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
        return "第" + workId + "工作组的类型为"+workGroupType+",数据为\n" + "In:" + Arrays.toString(in.toArray()) + "\n" +
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
     * 检查当前的workGroup需要执行何种类型的任务
     * <p>
     * 1. 如果inout不为空，那么必定为转账业务测试，其他两种类型的测试不涉及到同列数据的操作
     * 2. 如果inout为空
     * + 如果in或out都为空那么，为order型业务
     * + 如果in和out的size都为1可以判定为in=k*out+b型的数据
     * + 如果in或者out不为空，且有一个的size大于1，那么可以认定为还是转账业务
     */
    void check() {
        if (!inout.isEmpty()) {
            workGroupType = WorkGroupType.remittance;
        } else {
            if (in.isEmpty() || out.isEmpty()) {
                workGroupType = WorkGroupType.order;
            } else {
                if (in.size() == 1 && out.size() == 1) {
                    workGroupType = WorkGroupType.function;
                } else {
                    workGroupType = WorkGroupType.remittance;
                }
            }
        }

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
        if(workGroupType==WorkGroupType.function){
            k=(int)(in.get(0).getBeginSum()/out.get(0).getBeginSum());
        }
    }

    public int getK(){
        return k;
    }

    void checkCorrect() {
        if(workGroupType==WorkGroupType.remittance){
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
            if (!DoubleTuple.df.format(beginSum).equals(DoubleTuple.df.format(endSum))) {
                System.out.println("工作组" + workId + "前后和不一致，前和为" + beginSum + ",后和为" + endSum);
            } else {
                System.out.println("工作组" + workId + "前后和一致");
            }
        }else if(workGroupType==WorkGroupType.function){
            double preB=in.get(0).getBeginSum()-k*out.get(0).getBeginSum();
            double postB=in.get(0).getEndSum()-k*out.get(0).getEndSum();
            if(!DoubleTuple.df.format(preB).equals(DoubleTuple.df.format(postB))){
                System.out.println("工作组" + workId + "没有满足一次函数关系,k是"+k+"前b是"+preB+"后b是"+postB);
            }else {
                System.out.println("工作组" + workId + "满足一次函数关系");
            }

        }


    }

    public enum WorkGroupType {
        /*转账*/
        remittance,
        /*kx+b的函数式关系*/
        function,
        /*order关系*/
        order
    }
}
