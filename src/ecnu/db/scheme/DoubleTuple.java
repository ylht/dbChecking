package ecnu.db.scheme;


/**
 * @author wangqingshuai
 * double的tuple相关类
 */
public class DoubleTuple extends AbstractTuple {
    private double min;
    private double tupleRange;

    DoubleTuple(int tupleIndex, double min, double tupleRange) {
        this.tupleIndex = tupleIndex;
        this.min = min;
        this.tupleRange = tupleRange;
    }

    @Override
    public String getTableSQL() {
        int decimalLength = String.valueOf(min + tupleRange).length() + 2;
        return "tp" + tupleIndex + " DECIMAL (" + decimalLength + ",2),";
    }

    @Override
    public Object getValue(boolean processingTableData) {
        if (processingTableData) {
            return R.nextDouble() * tupleRange + min;
        } else {
            return R.nextDouble() * tupleRange / RANGE_RANDOM_COUNT;
        }
    }

    @Override
    public Object getMaxValue() {
        return min + tupleRange;
    }
}
