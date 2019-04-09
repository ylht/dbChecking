package ecnu.db.scheme;


import java.text.DecimalFormat;

/**
 * @author wangqingshuai
 * double的tuple相关类
 */
public class DecimalColumn extends AbstractColumn {

    public static DecimalFormat df = new DecimalFormat("0.00");

    private int pointLength = -1;

    DecimalColumn(int min, int range) {
        super(min, range);
    }

    public DecimalColumn(int min, int range, int pointLength) {
        super(min, range);
        this.pointLength = pointLength;
    }

    @Override
    public String getTableSQL() {
        if (pointLength == -1) {
            return "FLOAT";
        } else {
            int decimalLength = String.valueOf(min + range).length() + pointLength;
            return "DECIMAL (" + decimalLength + ',' + pointLength + ')';
        }

    }

    @Override
    public Object getValue(boolean processingTableData) {
        if (processingTableData) {
            return df.format(R.nextDouble() * range + min);
        } else {
            return df.format(R.nextDouble() * range / RANGE_RANDOM_COUNT);
        }
    }
}
