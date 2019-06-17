package ecnu.db.schema;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Random;

public class DateColumn extends AbstractColumn {
    /**
     * 创建一个格式化日期对象
     */
    private static final DateFormat SIMPLE_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    DateColumn(int range) {
        super(range, ColumnType.DATE);
    }

    @Override
    public String getTableSQL() {
        return "DATETIME";
    }

    @Override
    public Object getValue() {
        return SIMPLE_DATE_FORMAT.format(System.currentTimeMillis() + 1000 * new Random().nextInt(range));
    }
}
