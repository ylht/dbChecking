package ecnu.db.config;

import ecnu.db.schema.AbstractColumn;
import org.dom4j.Node;

import java.util.List;
import java.util.Objects;

public class TransactionConfig extends ReadConfig {

    private static String configFileDirectory;

    private TransactionConfig(String fileName) {
        super(fileName);
    }

    public static void setConfigFileDirectory(String configFileDirectoryName) {
        configFileDirectory = configFileDirectoryName;
    }

    public synchronized static TransactionConfig getConfig(String configName) {
        return new TransactionConfig(Objects.requireNonNullElse(configFileDirectory, "config/transactionConfig/") + configName);
    }


    public boolean getColumnFromSameTable() {
        String columnFromSameTableConfig = document.valueOf("transaction/columnFromSameTable");
        if (columnFromSameTableConfig.length() != 0) {
            return Boolean.valueOf(columnFromSameTableConfig);
        } else {
            return false;
        }
    }

    public boolean getWholeTable() {
        String wholeTableConfig = document.valueOf("transaction/wholeTable");
        if (wholeTableConfig.length() != 0) {
            return Boolean.valueOf(wholeTableConfig);
        } else {
            return false;
        }
    }


    public int getMinColumnNum() {
        return Integer.valueOf(document.valueOf("transaction/minColumnNum"));
    }

    public int getColumnNumForTransaction() throws Exception {
        return getValueFromHistogram("transaction/columnNum");
    }

    public Byte getTransactionCheckType() throws Exception {
        return getCheckType("transaction/transactionCheckType");
    }


    public Byte getConfigCheckType(String configName) throws Exception {
        return getCheckType("transaction/configType/" + configName);
    }

    private byte getCheckType(String configName) throws Exception {
        int checkType = 0;
        String checkTypeString = document.valueOf(configName);
        for (int i = 0; i < checkTypeString.length(); i++) {
            if (checkTypeString.charAt(i) == '1') {
                checkType += Math.pow(2, checkTypeString.length() - 1 - i);
            }
            if (checkTypeString.charAt(i) != '1' && checkTypeString.charAt(i) != '0') {
                throw new Exception("配置项错误");
            }
        }
        return (byte) checkType;
    }


    /**
     * @return 该事务需要检测的列的类型，支持int,decimal,varchar,float,date五种
     * @throws Exception 1. 错误的类型配置
     *                   2. 错误的概率
     *                   3. 未配置该项
     */
    public AbstractColumn.ColumnType getColumnTypeForTransacion() throws Exception {
        double radio = R.nextDouble();
        List<Node> nodeList = document.selectNodes("transaction/columnType/HistogramItem");
        double old = 0;
        for (Node node : nodeList) {
            old += Double.valueOf(node.valueOf("ratio"));
            if (radio < old) {
                switch (node.valueOf("type")) {
                    case "int":
                        return AbstractColumn.ColumnType.INT;
                    case "decimal":
                        return AbstractColumn.ColumnType.DECIMAL;
                    case "date":
                        return AbstractColumn.ColumnType.DATE;
                    case "float":
                        return AbstractColumn.ColumnType.FLOAT;
                    case "varchar":
                        return AbstractColumn.ColumnType.VARCHAR;
                    default:
                        throw new Exception("没有名为" + node.valueOf("type") + "的数据类型");
                }
            }
        }
        throw new Exception("配置文件中的概率和小于1,或者不存在该配置项");

    }


    public Integer getRangeRandomCount() {
        return Integer.valueOf(document.valueOf("transaction/rangeRandomCount"));
    }


    public int getK() throws Exception {
        return getValueFromHistogram("transaction/functionK");
    }

    public boolean addOrNot() {
        return R.nextDouble() < Double.valueOf(document.valueOf("transaction/add"));
    }


    public int getOrderMaxCount() {
        return Integer.valueOf(document.valueOf("transaction/orderMaxCount"));
    }

    public long getSleepMills() {
        return Long.valueOf(document.valueOf("transaction/sleepMills"));
    }


    public double getReadWriteRadio() {
        return Double.valueOf(document.valueOf("transaction/readWriteRadio"));
    }


}
