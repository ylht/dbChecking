package ecnu.db.scheme;


/**
 * @author wangqingshuai
 * int的tuple相关类
 */
public class IntColumn extends AbstractColumn {

    IntColumn(int min, int range) {
        super(min, range);
    }

    @Override
    public String getTableSQL() {
        return "INT";
    }

    @Override
    public Object getValue(boolean processingTableData) {
        if (processingTableData) {
            return R.nextInt(range) + min;
        } else {
            return R.nextInt(range / RANGE_RANDOM_COUNT);
        }
    }
}
