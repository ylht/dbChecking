package ecnu.db.scheme;

import ecnu.db.utils.LoadConfig;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;

/**
 * @author wangqingshuai
 * 数据库表相关的类，存储数据库表的所有信息
 * <p>
 * 插入数据时，采用不完全插入，主键key mod KeyRange为
 */
public class Table {

    private final static Random R = new Random();

    /**
     * 主键数量，默认为1
     */
    private final static int KEY_NUM = 1;

    /**
     * 记录列，用于记录各种值
     */
    private final static String[] RECORD_COLUMNS =
            new String[]{"checkNoCommit INT default 0", "checkRepeatableRead INT default 0"};

    private int tableIndex;
    private int tableSize;
    private int foreignKeyNum;


    private ArrayList<Integer> keys = new ArrayList<>();
    private ArrayList<Integer> foreignKeys;
    private ArrayList<AbstractColumn> columns = new ArrayList<>();

    private AtomicInteger currentLineNum=new AtomicInteger();


    public Table(int tableIndex,ArrayList<ArrayList<Integer>> allKeys) throws Exception {
        this.tableIndex = tableIndex;
        double tableSparsity = LoadConfig.getConfig().getTableSparsity();

        this.foreignKeyNum = Math.min(LoadConfig.getConfig().getForeignKeyNum(),allKeys.size());

        if(foreignKeyNum>0){
            ArrayList<Integer> arrays=new ArrayList<>();
            for (int i = 0; i < allKeys.size(); i++) {
                arrays.add(i);
            }
            Collections.shuffle(arrays);
            this.foreignKeys=new ArrayList<>(arrays.subList(0,foreignKeyNum));
            for (int i = 0; i < foreignKeyNum; i++) {
                columns.add(new IntColumn(allKeys.get(foreignKeys.get(i))));
            }
        }


        //从配置文件中获取基本信息
        tableSize = LoadConfig.getConfig().getTableSize();
        for (int i = 0; i < tableSize; i++) {
            if(R.nextDouble()< tableSparsity){
                keys.add(i);
            }
        }
        allKeys.add(keys);



        int tableColumnNum = LoadConfig.getConfig().getColumnNum();
        for (int i = 0; i < tableColumnNum; i++) {
            //数据的tuple从第二列开始，第一列作为主键列
            String type = LoadConfig.getConfig().getColumnType();
            switch (type) {
                case "int":
                    columns.add(new IntColumn(LoadConfig.getConfig().getRange("int")));
                    break;
                case "decimal":
                    columns.add(new DecimalColumn(LoadConfig.getConfig().getRange("decimal"),
                            LoadConfig.getConfig().getDecimalPoint()));
                    break;
                case "float":
                    columns.add(new DecimalColumn(LoadConfig.getConfig().getRange("decimal")));
                    break;
                case "varchar":
                    columns.add(new VarcharColumn(LoadConfig.getConfig().getRange("varchar")));
                    break;
                case "datetime":
                    columns.add(new DateColumn(LoadConfig.getConfig().getRange("date")));
                    break;
                default:
                    throw new Exception("配置文件错误,匹配到的项为：" + type);
            }
        }


    }

    /**
     * @return 存在主键的一个拷贝
     */
    public ArrayList<Integer> getKeys() {
        return keys;
    }

    /**
     * @return 一个随机的key，只保证数据生成时，该主键存在过。
     */
    public int getRandomKey() {
        return keys.get(R.nextInt(keys.size()));
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
        for (int i=0; i< foreignKeyNum; i++) {
            sql.append("FOREIGN KEY (tp").append(i+KEY_NUM).append(") REFERENCES t")
                    .append(foreignKeys.get(i)).append("(tp0),");
        }

        sql.append("PRIMARY KEY ( `tp0` ));");

        return sql.toString();
    }

    public int getTableIndex() {
        return tableIndex;
    }

    public int getTableColSize() {
        return columns.size();
    }

    public int getTableRecordSize() {
        return RECORD_COLUMNS.length;
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
        int temp=currentLineNum.getAndIncrement();
        if(temp>=keys.size()){
            return null;
        }
        lineRecord[0]=keys.get(temp);
        return getObjects(lineRecord);
    }

    /**
     * 用于插入数据时获取每一行的数据
     *
     * @return 一行的数据
     */
    public Object[] getInsertValue() {
        Object[] lineRecord = new Object[KEY_NUM + columns.size() + RECORD_COLUMNS.length];
        for (int i = KEY_NUM; i <= RECORD_COLUMNS.length; i++) {
            lineRecord[lineRecord.length - i] = 0;
        }
        lineRecord[0] = R.nextInt(tableSize);
        return getObjects(lineRecord);
    }

    private Object[] getObjects(Object[] lineRecord) {
        for (int i = KEY_NUM; i < lineRecord.length - RECORD_COLUMNS.length; i++) {
            lineRecord[i] = columns.get(i - KEY_NUM).getValue(true);
        }
        return lineRecord;
    }

    public Double getTransactionValue(int tupleIndex) {
        return Double.valueOf(columns.get(tupleIndex - KEY_NUM).getValue(false).toString());
    }

    public Double getRandomValue(int tupleIndex) {
        return Double.parseDouble(columns.get(tupleIndex - KEY_NUM).getValue(true).toString());
    }
}
