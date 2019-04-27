package ecnu.db.scheme;


import java.text.DecimalFormat;

/**
 * @author wangqingshuai
 * double的tuple相关类
 */
public class DecimalColumn extends AbstractColumn {

    private static DecimalFormat df;
    private static int allPointLength = -1;

    DecimalColumn(int range, int pointLength) {
        super(range, ColumnType.DECIMAL);
        if (allPointLength < 0) {
            allPointLength = pointLength;
            df = new DecimalFormat("0." + "0".repeat(Math.max(0, allPointLength)));
        }
    }

    public static DecimalFormat getDf() {
        return df;
    }

    @Override
    public String getTableSQL() {
        int decimalLength = String.valueOf(range).length() + allPointLength;
        return "DECIMAL (" + decimalLength + ',' + allPointLength + ')';
    }

    @Override
    public Object getValue() {
        return df.format(R.nextDouble() * range);
    }
}
