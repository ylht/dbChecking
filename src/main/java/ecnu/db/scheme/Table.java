package ecnu.db.scheme;

import ecnu.db.utils.LoadConfig;
import org.apache.commons.math3.distribution.ZipfDistribution;
import org.dom4j.Node;

import java.util.ArrayList;
import java.util.List;

/**
 * @author wangqingshuai
 * 数据库表相关的类，存储数据库表的所有信息
 */
public class Table {

    private ArrayList<AbstractTuple> tuples = new ArrayList<>();
    private int tableIndex;
    private int tableSize;
    private int currentValueLine = 0;
    private Object[] lineRecord;
    private ZipfDistribution zf;

    public Table(int tableIndex, int tableSize) {
        this.tableIndex = tableIndex;
        this.tableSize = tableSize;
        this.zf = new ZipfDistribution(tableSize, 1);
        List<Node> nodes = LoadConfig.getConfig().getTableTupleInfo(tableIndex);
        this.lineRecord = new Object[nodes.size() + 1];

        //数据的tuple从第二列开始，第一列作为主键列
        int i = 1;
        for (Node node : nodes) {
            switch (LoadConfig.getConfig().getType()) {
                case "int":
                    tuples.add(new IntTuple(i, Integer.parseInt(node.valueOf("min")),
                            Integer.parseInt(node.valueOf("range"))));
                    break;
                case "double":
                    tuples.add(new DoubleTuple(i, Integer.parseInt(node.valueOf("min")),
                            Integer.parseInt(node.valueOf("range"))));
                    break;
                default:
                    System.out.println("配置文件错误");
                    System.exit(-1);
            }
            i++;
        }
    }

    public int getRandomKey() {
        return zf.sample();
    }

    public String getSQL() {
        StringBuilder sql = new StringBuilder("CREATE TABLE t" + tableIndex + "(tp0 INT,");
        for (AbstractTuple tuple : tuples) {
            sql.append(tuple.getTableSQL());
        }
        //将SQL句尾的逗号替换为括号
        return sql + "PRIMARY KEY ( `tp0` ));";
    }

    public int getTableIndex() {
        return tableIndex;
    }

    public Object[] getValue() {
        if (currentValueLine == tableSize) {
            return null;
        } else {
            lineRecord[0] = currentValueLine++;
            for (int i = 1; i < lineRecord.length; i++) {
                lineRecord[i] = tuples.get(i - 1).getValue(true);
            }
            return lineRecord;
        }
    }

    public Double getTransactionValue(int tupleIndex) {
        return Double.valueOf(tuples.get(tupleIndex - 1).getValue(false).toString());
    }

    public Double getMaxValue(int tupleIndex) {
        return Double.parseDouble(tuples.get(tupleIndex - 1).getMaxValue().toString());
    }

    public Double getRandomValue(int tupleIndex) {
        return Double.parseDouble(tuples.get(tupleIndex - 1).getValue(true).toString());
    }
}
