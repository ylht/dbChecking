package ecnu.db.scheme;


import java.text.DecimalFormat;
import java.text.Format;

/**
 * @author wangqingshuai
 * double的tuple相关类
 */
public class DecimalColumn extends AbstractColumn {

    public static DecimalFormat df;

    private static int allPointLength = -1;

    private boolean isFloat=true;

    DecimalColumn(int range) {
        super(range);
    }

    DecimalColumn(int range, int pointLength) {
        super(range);
        isFloat=false;
        if(allPointLength<0){
            allPointLength = pointLength;
            df=new DecimalFormat("0." + "0".repeat(Math.max(0, allPointLength)));
        }
    }

    @Override
    public String getTableSQL() {
        if (isFloat) {
            return "FLOAT";
        } else {
            int decimalLength = String.valueOf(range).length() + allPointLength;
            return "DECIMAL (" + decimalLength + ',' + allPointLength + ')';
        }

    }

    @Override
    public Object getValue(boolean processingTableData) {
        if (processingTableData) {
            return df.format(R.nextDouble() * range);
        } else {
            return df.format(R.nextDouble() * range / RANGE_RANDOM_COUNT);
        }
    }
}
