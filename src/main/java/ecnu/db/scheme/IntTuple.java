package ecnu.db.scheme;


/**
 * @author wangqingshuai
 * int的tuple相关类
 */
public class IntTuple extends AbstractTuple {
    private int min;
    private int tupleRange;

    IntTuple(int tupleIndex, int min, int tupleRange) {
        this.tupleIndex = tupleIndex;
        this.min = min;
        this.tupleRange = tupleRange;
    }

    @Override
    public String getTableSQL() {
        return "tp" + tupleIndex + " INT,";
    }

    @Override
    public Object getValue(boolean processingTableData) {
        if (processingTableData) {
            return R.nextInt(tupleRange) + min;
        } else {
            return R.nextInt(tupleRange / RANGE_RANDOM_COUNT);
        }
    }

    @Override
    public Object getMaxValue() {
        return min + tupleRange;
    }
}
