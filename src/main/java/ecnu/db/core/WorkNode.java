package ecnu.db.core;

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
    private ArrayList<Integer> addValueList;
    private ArrayList<Integer> subValueList;


    public void setAddValueList(ArrayList<Integer> addValueList) {
        Collections.shuffle(addValueList);
        this.addValueList = addValueList;
    }

    public void setSubValueList(ArrayList<Integer> subValueList) {
        Collections.shuffle(subValueList);
        this.subValueList = subValueList;
    }

    public WorkNode(int tableIndex, int tupleIndex) {
        this.tableIndex = tableIndex;
        this.tupleIndex = tupleIndex;
    }

    public ArrayList<Integer> getAddValueList() {
        return addValueList;
    }

    public ArrayList<Integer> getSubValueList() {
        return subValueList;
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
