package ecnu.db.utils;

import org.apache.logging.log4j.LogManager;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Node;
import org.dom4j.io.SAXReader;

/**
 * @author wangqingshuai
 * 读取配置文件的类
 */
public class LoadConfig {
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


    //运行的相关信息

    public int getThreadNum() {
        return 8 * Runtime.getRuntime().availableProcessors();
    }

    public int getRunCount() {
        String xpath = "//generator/runCount";
        Node list = document.selectNodes(xpath).get(0);
        return Integer.valueOf(list.getText());
    }

    public Integer getRangeRandomCount() {
        return Integer.valueOf(document.valueOf("//generator/rangeRandomCount"));
    }

    //表格信息

    public int getTableNum() {
        return Integer.valueOf(document.valueOf("//generator/table/num"));
    }

    public int getTableSize() {
        return Integer.valueOf(document.valueOf("//generator/table/tableSize"));
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

    public int getTupleNum() {
        return Integer.valueOf(document.valueOf("//generator/tuple/num"));
    }

    public double getTupleMin() {
        return Integer.valueOf(document.valueOf("//generator/tuple/min"));
    }

    public double getTupleRange() {
        return Integer.valueOf(document.valueOf("//generator/tuple/max")) - getTupleMin();
    }
}
