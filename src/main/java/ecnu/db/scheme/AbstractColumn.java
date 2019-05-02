package ecnu.db.scheme;


import java.util.Random;

/**
 * @author wangqingshuai
 * 该类为所有tuple的抽象类
 */

public abstract class AbstractColumn {

    /**
     * 所有的tuple同用一个随机数生成器
     */
    final static Random R = new Random();

    /**
     * 在int,decimal,float,date中代表数值范围区间，在varchar中代表长度范围区间
     */
    int range;
    private ColumnType columnType;

    AbstractColumn(int range, ColumnType columnType) {
        this.range = range;
        this.columnType = columnType;
    }

    public ColumnType getColumnType() {
        return columnType;
    }

    public int getRange() {
        return range;
    }

    /**
     * 生成定义该tuple约束的SQL语句
     *
     * @return 一条SQL语句，按顺序包括，列名，数据类型，和句尾逗号
     */
    public abstract String getTableSQL();

    /**
     * 获取在这个字段上的数据值，有两种模式
     * 此时的数据填充会在数据的约束范围，即min到min+tupleRange之间，随机生成数据，作为填充
     *
     * @return 返回数据值对象
     */
    abstract Object getValue();

    public enum ColumnType {
        /**
         * 表示column的所有 包括以下五种
         */
        INT, DECIMAL, FLOAT, VARCHAR, DATE
    }
}
