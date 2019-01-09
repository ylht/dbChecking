package ecnu.db.tuple;

import ecnu.db.utils.LoadConfig;

import java.util.Random;

/**
 * @author wangqingshuai
 * 该类为所有tuple的抽象类
 */

public abstract class AbstractTuple {
    /**
     * 事务在range范围内随机得到值时，range的大小，计算公式为
     * range= tupleRange/ RANGE_RANDOM_COUNT
     */
    final static int RANGE_RANDOM_COUNT = LoadConfig.getConfig().getRangeRandomCount();

    /**
     * 所有的tuple同用一个随机数生成器
     */
    final static Random R = new Random();

    /**
     * tuple的名称标示，用于唯一的标示一个列，字符标示为"'tp'+tupleIndex"
     */
    int tupleIndex;

    /**
     * 生成定义该tuple约束的SQL语句
     *
     * @return 一条SQL语句，按顺序包括，列名，数据类型，和句尾逗号
     */
    public abstract String getTableSQL();

    /**
     * 获取在这个字段上的数据值，有两种模式
     * 1. true 为table填充数据生成
     * 此时的数据填充会在数据的约束范围，即min到min+tupleRange之间，随机生成数据，作为填充
     * 2. false 为事务操作时的数据生成
     * 此时的数据生成会在range范围内，即tupleRange /RANGE_RANDOM_COUNT之间，随机生成数据，这里的n暂定为10
     *
     * @param processingTableData 是否为table填充生成数据，
     * @return 返回数据值对象
     */
    public abstract Object getValue(boolean processingTableData);

}
