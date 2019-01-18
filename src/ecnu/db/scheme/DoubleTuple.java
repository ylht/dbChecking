package ecnu.db.scheme;


import java.text.DecimalFormat;

/**
 * @author wangqingshuai
 * double的tuple相关类
 */
public class DoubleTuple extends AbstractTuple {
    private double min;
    private double tupleRange;
    public static DecimalFormat df = new DecimalFormat("0.00");
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
            return df.format(R.nextDouble() * tupleRange + min);
        } else {
            return df.format(R.nextDouble() * tupleRange / RANGE_RANDOM_COUNT);
        }
    }

    @Override
    public Object getMaxValue() {
        return df.format(min + tupleRange);
    }
}
