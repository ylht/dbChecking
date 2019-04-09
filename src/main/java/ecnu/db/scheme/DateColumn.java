package ecnu.db.scheme;

import java.text.DateFormat;
import java.text.SimpleDateFormat;

public class DateColumn extends AbstractColumn {
    /**
     * 创建一个格式化日期对象
     */
    private static final DateFormat SIMPLE_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    DateColumn(int min, int range) {
        super(min, range);
    }

    @Override
    public String getTableSQL() {
        return "DATETIME";
    }

    @Override
    public Object getValue(boolean processingTableData) {
        return SIMPLE_DATE_FORMAT.format(System.currentTimeMillis() + min + R.nextInt(range));
    }
}
