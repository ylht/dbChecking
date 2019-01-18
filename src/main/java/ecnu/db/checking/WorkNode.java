package ecnu.db.checking;

import ecnu.db.utils.LoadConfig;

import java.util.ArrayList;
import java.util.Collections;

/**
 * @author wangqingshuai
 * 标记转移的列，并记录该列的起始和和终止和
 */
public class WorkNode {
    private int tableIndex;
    private int tupleIndex;
    private Double beginSum;
    private Double endSum;
    private ArrayList<Integer> addValueList;
    private ArrayList<Integer> subValueList;


    public WorkNode(int tableIndex, int tupleIndex) {
        this.tableIndex = tableIndex;
        this.tupleIndex = tupleIndex;
        int tableLine = LoadConfig.getConfig().getTableSize()[tableIndex];
        ArrayList<Integer> valueList = new ArrayList<>();
        for (int i = 0; i < tableLine; i++) {
            valueList.add(i);
        }
        Collections.shuffle(valueList);
        addValueList = new ArrayList<>(valueList);
        Collections.shuffle(valueList);
        subValueList = new ArrayList<>(valueList);
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
