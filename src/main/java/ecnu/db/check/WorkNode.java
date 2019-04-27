package ecnu.db.check;

import java.util.ArrayList;

/**
 * @author wangqingshuai
 * 标记转移的列，并记录该列的起始和和终止和
 */
public class WorkNode {
    private int tableIndex;
    private int columnIndex;

    private ArrayList<Integer> keys;
    private int range;

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


    public WorkNode(int tableIndex, int columnIndex,
                    ArrayList<Integer> keys, int range) {
        this.tableIndex = tableIndex;
        this.columnIndex = columnIndex;
        this.keys = keys;
        this.range = range;

    }

    public ArrayList<Integer> getKeys() {
        return keys;
    }

    public int getRange() {
        return range;
    }

    public int getTableIndex() {
        return tableIndex;
    }

    public int getColumnIndex() {
        return columnIndex;
    }

    public int getOrderNum() {
        return orderNum;
    }

    public void setOrderNum(int orderNum) {
        this.orderNum = orderNum;
    }

    public Double getBeginSum() {
        return beginSum;
    }

    public void setBeginSum(Double beginSum) {
        this.beginSum = beginSum;
    }

    public Double getEndSum() {
        return endSum;
    }

    public void setEndSum(Double endSum) {
        this.endSum = endSum;
    }


    @Override
    public String toString() {
        return "[" + tableIndex + "," + columnIndex + "," + beginSum + "," + endSum + "]";
    }

}
