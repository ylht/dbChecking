package ecnu.db.scheme;


import java.text.DecimalFormat;

/**
 * @author wangqingshuai
 * double的tuple相关类
 */
public class DecimalColumn extends AbstractColumn {

    private static DecimalFormat df;
    private static int allPointLength = -1;
    private int decimalLength;

    DecimalColumn(int range, int pointLength) {
        super(range, ColumnType.DECIMAL);
        decimalLength=range;
        this.range =(int) Math.pow(10, range-1);
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
        return "DECIMAL (" + (decimalLength + allPointLength) + ',' + allPointLength + ')';
    }

    @Override
    public Object getValue() {
        return df.format(R.nextDouble() * range);
    }
}
