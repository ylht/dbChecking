package ecnu.db.scheme;

import ecnu.db.utils.LoadConfig;
import org.apache.commons.math3.distribution.ZipfDistribution;

import java.util.ArrayList;
import java.util.Random;

/**
 * @author wangqingshuai
 * 数据库表相关的类，存储数据库表的所有信息
 * <p>
 * 插入数据时，采用不完全插入，主键key mod KeyRange为
 */
public class Table {

    private final static Random R = new Random();
    private ArrayList<AbstractTuple> tuples = new ArrayList<>();
    private int tableIndex;
    private int tableSize;
    private double tableSparsity;
    private ArrayList<Integer> keys = new ArrayList<>();
    private int currentValueLine = 0;
    private Object[] lineRecord;
    private static final int RECORD_COL=2;

    public Table(int tableIndex, int tableSize, int tupleSize) {
        this.tableIndex = tableIndex;
        this.tableSize = tableSize;
        //获取除主键外其他键值的数据信息
        this.tableSparsity = LoadConfig.getConfig().getTableSparsity();
        double min = LoadConfig.getConfig().getTupleMin();
        double range = LoadConfig.getConfig().getTupleRange();
        lineRecord = new Object[tupleSize + 1+RECORD_COL];
        lineRecord[lineRecord.length-1]=lineRecord[lineRecord.length-2]=0;
        //数据的tuple从第二列开始，第一列作为主键列
        switch (LoadConfig.getConfig().getType()) {
            case "int":
                for (int i = 0; i < tupleSize; i++) {
                    tuples.add(new IntTuple(i + 1, (int) min, (int) range));
                }
                break;
            case "double":
                for (int i = 0; i < tupleSize; i++) {
                    tuples.add(new DoubleTuple(i + 1, min, range));
                }
                break;
            default:
                System.out.println("配置文件错误");
                System.exit(-1);
        }
    }


    public ArrayList<Integer> getKeys() {
        return keys;
    }

    public int getRandomKey() {
        return R.nextInt(tableSize);
    }

    public String getSQL() {
        StringBuilder sql = new StringBuilder("CREATE TABLE t" + tableIndex + "(tp0 INT,");
        for (AbstractTuple tuple : tuples) {
            sql.append(tuple.getTableSQL());
        }
        //将SQL句尾的逗号替换为括号
        return sql + "checkNoCommit INT default 0," +
                "checkRepeatableRead INT default 0,PRIMARY KEY ( `tp0` ));";
    }

    public int getTableIndex() {
        return tableIndex;
    }

    public int getTableColSizeExceptKey() {
        return tuples.size();
    }

    public int getTableColSizeForInsert(){
        return tuples.size()+RECORD_COL;
    }

    public Object[] getValue() {
        if (currentValueLine == tableSize) {
            return null;
        } else {
            do {
                lineRecord[0] = currentValueLine++;
            }
            //如果随机到的数值小于给定的数值，则跳过该主键，给定数值为1时表为空
            while (R.nextDouble() < tableSparsity && currentValueLine != tableSize);
            if (currentValueLine == tableSize) {
                return null;
            }
            //记录所有一开始有值的主键
            keys.add((Integer) lineRecord[0]);
            return getObjects();
        }
    }

    public Object[] getInsertValue() {
        lineRecord[0] = R.nextInt(tableSize);
        return getObjects();
    }

    private Object[] getObjects() {
        for (int i = 1; i < lineRecord.length-RECORD_COL; i++) {
            lineRecord[i] = tuples.get(i - 1).getValue(true);
        }
        return lineRecord;
    }

    public Double getTransactionValue(int tupleIndex) {
        return Double.valueOf(tuples.get(tupleIndex - 1).getValue(false).toString());
    }

    public Double getRandomValue(int tupleIndex) {
        return Double.parseDouble(tuples.get(tupleIndex - 1).getValue(true).toString());
    }
}
