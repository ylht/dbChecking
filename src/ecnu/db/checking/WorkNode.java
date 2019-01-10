package ecnu.db.checking;

public class WorkNode {
    private int tableIndex;
    private int tupleIndex;

    public WorkNode(int tableIndex, int tupleIndex) {
        this.tableIndex = tableIndex;
        this.tupleIndex = tupleIndex;
    }

    public int getTableIndex() {
        return tableIndex;
    }

    public int getTupleIndex() {
        return tupleIndex;
    }

    @Override
    public String toString(){
        return "["+tableIndex+","+tupleIndex+"]";
    }
}
