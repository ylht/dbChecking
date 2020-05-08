package ecnu.db.schema;


import ecnu.db.check.CheckNode;
import ecnu.db.config.TableConfig;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author wangqingshuai
 * 数据库表相关的类，存储数据库表的所有信息
 * <p>
 * 插入数据时，采用不完全插入，主键key mod KeyRange为
 */
public class Table {

    /**
     * 主键数量，默认为1
     */
    private final static int KEY_NUM = 1;
    /**
     * 记录列，用于记录各种值
     */
    private final static String[] RECORD_COLUMNS =
            new String[]{"checkReadCommitted decimal(20,5) default 0", "checkRepeatableRead INT default 0"};
    private final Random R = new Random();
    private int tableIndex;
    private int tableSize;
    private int foreignKeyNum;
    private ArrayList<Integer> keys = new ArrayList<>();
    private ArrayList<Integer> foreignKeys;
    private ArrayList<AbstractColumn> columns = new ArrayList<>();
    private AtomicInteger currentLineNum = new AtomicInteger();
    private HashMap<AbstractColumn.ColumnType, ArrayList<CheckNode>> checkNodes =
            new HashMap<>(AbstractColumn.ColumnType.values().length);

    public Table(int tableIndex, ArrayList<ArrayList<Integer>> allKeys) throws Exception {
        this.tableIndex = tableIndex;
        double tableSparsity = TableConfig.getConfig().getTableSparsity();

        //获取外键数量
        this.foreignKeyNum = Math.min(TableConfig.getConfig().getForeignKeyNum(), allKeys.size());

        if (foreignKeyNum > 0) {
            ArrayList<Integer> arrays = new ArrayList<>();
            for (int i = 0; i < allKeys.size(); i++) {
                arrays.add(i);
            }
            Collections.shuffle(arrays);
            //记录外键引用表的index
            this.foreignKeys = new ArrayList<>(arrays.subList(0, foreignKeyNum));
            for (int i = 0; i < foreignKeyNum; i++) {
                columns.add(new IntColumn(allKeys.get(foreignKeys.get(i))));
            }
        }


        //从配置文件中获取基本信息
        tableSize = TableConfig.getConfig().getTableSize();
        for (int i = 0; i < tableSize; i++) {
            if (R.nextDouble() < tableSparsity) {
                keys.add(i);
            }
        }
        allKeys.add(keys);

        int decimalRange = TableConfig.getConfig().getRange("decimal");
        int decimalPoint = TableConfig.getConfig().getDecimalPoint();
        int tableColumnNum = TableConfig.getConfig().getColumnNum();
        //初始化checkNodes
        for (int i = 0; i < AbstractColumn.ColumnType.values().length; i++) {
            checkNodes.put(AbstractColumn.ColumnType.values()[i], new ArrayList<>());
        }

        for (int i = foreignKeyNum + 1; i < tableColumnNum + foreignKeyNum + 1; i++) {
            //数据的tuple从第二列开始，第一列作为主键列
            String type = TableConfig.getConfig().getColumnType();
            if (!"decimal".equals(type)) {
                int range = TableConfig.getConfig().getRange(type);
                switch (type) {
                    case "int":
                        checkNodes.get(AbstractColumn.ColumnType.INT).add(new CheckNode(tableIndex, i, keys, range));
                        columns.add(new IntColumn(range));
                        break;
                    case "float":
                        checkNodes.get(AbstractColumn.ColumnType.FLOAT).add(new CheckNode(tableIndex, i, keys, range));
                        columns.add(new FloatColumn(range));
                        break;
                    case "varchar":
                        checkNodes.get(AbstractColumn.ColumnType.VARCHAR).add(new CheckNode(tableIndex, i, keys, range));
                        columns.add(new VarcharColumn(range));
                        break;
                    case "datetime":
                        checkNodes.get(AbstractColumn.ColumnType.DATE).add(new CheckNode(tableIndex, i, keys, range));
                        columns.add(new DateColumn(range));
                        break;
                    default:
                        throw new Exception("配置文件错误,匹配到的项为：" + type);
                }
            } else {
                checkNodes.get(AbstractColumn.ColumnType.DECIMAL).add(new CheckNode(tableIndex, i, keys, decimalRange));
                columns.add(new DecimalColumn(decimalRange, decimalPoint));
            }
        }

        for (AbstractColumn.ColumnType value : AbstractColumn.ColumnType.values()) {
            Collections.shuffle(checkNodes.get(value));
        }

    }

    public HashMap<AbstractColumn.ColumnType, ArrayList<CheckNode>> getCheckNodes() {
        return checkNodes;
    }

    public void setNoCheckNodes() {
        this.checkNodes = null;
    }

    public ArrayList<Integer> getForeignKeys() {
        return foreignKeys;
    }

    public int getForeignKeyNum() {
        return foreignKeyNum;
    }

    /**
     * @return 主键的一个引用
     */
    public ArrayList<Integer> getKeys() {
        return keys;
    }


    /**
     * @return 创建该表格的Sql
     */
    public String getSQL() {
        StringBuilder sql = new StringBuilder("CREATE TABLE t" + tableIndex + "(tp0 INT,");
        int index = KEY_NUM;
        for (AbstractColumn tuple : columns) {
            sql.append("tp").append(index++).append(" ").append(tuple.getTableSQL()).append(',');
        }
        //增加用于辅助记录数据的列
        for (String recordColumn : RECORD_COLUMNS) {
            sql.append(recordColumn).append(',');
        }
        for (int i = 0; i < foreignKeyNum; i++) {
            sql.append("FOREIGN KEY (tp").append(i + KEY_NUM).append(") REFERENCES t")
                    .append(foreignKeys.get(i)).append("(tp0),");
        }

        sql.append("PRIMARY KEY ( tp0 ));");

        return sql.toString();
    }

    public int getTableIndex() {
        return tableIndex;
    }

    /**
     * 用于生成数据时获取每一行的数据
     *
     * @return 获取每一行的数据
     */
    public Object[] getValue() {
        Object[] lineRecord = new Object[KEY_NUM + columns.size() + RECORD_COLUMNS.length];
        for (int i = KEY_NUM; i <= RECORD_COLUMNS.length; i++) {
            lineRecord[lineRecord.length - i] = 0;
        }
        int temp = currentLineNum.getAndIncrement();
        if (temp >= keys.size()) {
            return null;
        }
        lineRecord[0] = keys.get(temp);
        for (int i = KEY_NUM; i < lineRecord.length - RECORD_COLUMNS.length; i++) {
            lineRecord[i] = columns.get(i - KEY_NUM).getValue();
        }
        return lineRecord;
    }

    public ArrayList<AbstractColumn> getColumns() {
        return columns;
    }
}
