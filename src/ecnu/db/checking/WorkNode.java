package ecnu.db.checking;

/**
 * @author wangqingshuai
 * 标记转移的列，并记录该列的起始和和终止和
 */
public class WorkNode {
    private int tableIndex;
    private int tupleIndex;
    private Double beginSum;
    private Double endSum;

    public WorkNode(int tableIndex, int tupleIndex) {
        this.tableIndex = tableIndex;
        this.tupleIndex = tupleIndex;
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
