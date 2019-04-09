package ecnu.db.utils;

import org.apache.logging.log4j.LogManager;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Node;
import org.dom4j.io.SAXReader;

import java.util.List;
import java.util.Random;

/**
 * @author wangqingshuai
 * 读取配置文件的类
 */
public class LoadConfig {
    private static final Random R = new Random();
    private static LoadConfig instance;
    private Document document;

    private LoadConfig(String fileName) {
        try {
            SAXReader reader = new SAXReader();
            document = reader.read(fileName);
        } catch (DocumentException e) {
            LogManager.getLogger().error(e);
        }
    }

    public static LoadConfig getConfig() {
        return instance;
    }

    public static void loadConfig(String configFile) {
        instance = new LoadConfig(configFile);
    }

    private int getValueFromHistogram(String histogramName) throws Exception {
        double radio = R.nextDouble();
        List<Node> nodeList = document.selectNodes("generator/schema/" + histogramName + "/HistogramItem");
        double old = 0;
        for (Node node : nodeList) {
            old += Double.valueOf(node.valueOf("ratio"));
            if (radio < old) {
                int min = Integer.valueOf(node.valueOf("minValue"));
                int max = Integer.valueOf(node.valueOf("maxValue"));
                return min + R.nextInt(max - min + 1);
            }
        }
        throw new Exception("此直方图的概率和小于1");
    }


    //事务信息

    public int getK() {
        return Integer.valueOf(document.valueOf("//generator/functionK"));
    }

    //运行的相关信息

    public int getThreadNum() {
        return 4 * Runtime.getRuntime().availableProcessors();
    }

    public int getRunCount() {
        return Integer.valueOf(document.valueOf("//generator/runCount"));
    }

    public Integer getRangeRandomCount() {
        return Integer.valueOf(document.valueOf("//generator/rangeRandomCount"));
    }

    //表格信息

    public int getTableNum() {
        return Integer.valueOf(document.valueOf("//generator/table/num"));
    }

    public int getTableSize() throws Exception {
        return getValueFromHistogram("tableSizeHistogram");
    }

    /**
     * @return 表格的稀疏度，为0时为生成全部的表键值区间，为1时为全部不生成，采用
     * random的方式，不确保最终的表大小为准确的keyRange*TableSparsity值，只能
     * 保证大概在这个区间范围内
     */
    public Double getTableSparsity() {
        return Double.valueOf(document.valueOf("//generator/table/tableSparsity"));
    }

    public String getType() {
        return document.valueOf("//generator/type");
    }

    public int getColumnNum() throws Exception {
        return getValueFromHistogram("columnNumHistogram");
    }

    public int getTupleMin() {
        return Integer.valueOf(document.valueOf("//generator/tuple/min"));
    }

    public int getTupleRange() {
        return Integer.valueOf(document.valueOf("//generator/tuple/max")) - getTupleMin();
    }
}
