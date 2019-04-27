package ecnu.db.config;

import org.dom4j.Node;

import java.util.List;

public class TableConfig extends ReadConfig {
    private static TableConfig config;

    private TableConfig(String fileName) {
        super(fileName);
    }

    public synchronized static TableConfig getConfig() {
        if (config == null) {
            config = new TableConfig("config/TableConfig.xml");
        }
        return config;
    }

    public static void setConfig(String configFileName) {
        config = new TableConfig(configFileName);
    }

    public double getZipf() {
        return Double.valueOf(document.valueOf("schema/zipf"));
    }

    public int getTableNum() throws Exception {
        return getValueFromHistogram("schema/tableNumHistogram");
    }

    public int getMaxTableNum() {
        List<Node> nodeList = document.selectNodes("schema/tableNumHistogram/HistogramItem");
        int max = 0;
        for (Node node : nodeList) {
            int temp = Integer.valueOf(node.valueOf("maxValue"));
            max = temp > max ? temp : max;
        }
        return max;
    }

    public int getColumnNum() throws Exception {
        return getValueFromHistogram("schema/columnNumHistogram");
    }

    public int getTableSize() throws Exception {
        return getValueFromHistogram("schema/tableSizeHistogram");
    }

    public int getForeignKeyNum() throws Exception {
        return getValueFromHistogram("schema/foreignKeyNumHistogram");
    }

    /**
     * @return 表格的稀疏度，为0时为生成全部的表键值区间，为1时为全部不生成，采用
     * random的方式，不确保最终的表大小为准确的keyRange*TableSparsity值，只能
     * 保证大概在这个区间范围内
     */
    public Double getTableSparsity() {
        return Double.valueOf(document.valueOf("schema/tableSparsity"));
    }

    public String getColumnType() throws Exception {
        double radio = R.nextDouble();
        List<Node> nodeList = document.selectNodes("schema/dataType2OccurProbability/item");
        double old = 0;
        for (Node node : nodeList) {
            old += Double.valueOf(node.valueOf("probability"));
            if (radio < old) {
                return node.valueOf("dataType");
            }
        }
        throw new Exception("概率和小于1，导致无法匹配到所需类型");
    }

    public int getDecimalPoint() {
        return R.nextInt(Integer.valueOf(document.valueOf("schema/decimal/pointMax")));
    }


    /**
     * 根据类型返回范围值，对于int,decimal,float，从0开始到range的范围，而对于
     *
     * @param type 当前需要获取数值范围的类型
     * @return 返回range范围
     * @throws Exception 配置文件错误
     */
    public int getRange(String type) throws Exception {
        return getValueFromHistogram("schema/" + type);
    }
}
