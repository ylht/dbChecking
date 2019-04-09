package ecnu.db.work.group;

import org.apache.commons.math3.distribution.ZipfDistribution;

import java.util.ArrayList;
import java.util.Collections;

/**
 * @author wangqingshuai
 * 标记转移的列，并记录该列的起始和和终止和
 */
public class WorkNode {
    private int tableIndex;
    private int tupleIndex;
    /**
     * 该列开始计算之前的和
     */
    private Double beginSum;
    /**
     * 该列计算完成之后的和
     */
    private Double endSum;
    /**
     * 如果该节点在order事务中，记录改值
     */
    private int orderNum;
    private ArrayList<Integer> addValueList;
    private ArrayList<Integer> subValueList;
    private ZipfDistribution addZf;
    private ZipfDistribution subZf;
    WorkNode(int tableIndex, int tupleIndex) {
        this.tableIndex = tableIndex;
        this.tupleIndex = tupleIndex;
    }

    int getOrderNum() {
        return orderNum;
    }

    void setOrderNum(int orderNum) {
        this.orderNum = orderNum;
    }

    public int getAddKey() {
        return addValueList.get(addZf.sample() - 1);
    }

    void setAddValueList(ArrayList<Integer> addValueList) {
        Collections.shuffle(addValueList);
        this.addValueList = addValueList;
        addZf = new ZipfDistribution(addValueList.size(), 1);
    }

    public int getSubKey() {
        return subValueList.get(subZf.sample() - 1);
    }

    void setSubValueList(ArrayList<Integer> subValueList) {
        Collections.shuffle(subValueList);
        this.subValueList = subValueList;
        subZf = new ZipfDistribution(subValueList.size(), 1);
    }

    Double getBeginSum() {
        return beginSum;
    }

    void setBeginSum(Double beginSum) {
        this.beginSum = beginSum;
    }

    Double getEndSum() {
        return endSum;
    }

    void setEndSum(Double endSum) {
        this.endSum = endSum;
    }

    public int getTableIndex() {
        return tableIndex;
    }

    public int getTupleIndex() {
        return tupleIndex;
    }

    @Override
    public String toString() {
        return "[" + tableIndex + "," + tupleIndex + "," + beginSum + "," + endSum + "]";
    }

}
