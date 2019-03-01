package ecnu.db.utils;

import ecnu.db.core.WorkGroup;
import ecnu.db.core.WorkNode;
import org.apache.logging.log4j.LogManager;
import org.dom4j.*;
import org.dom4j.io.SAXReader;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;

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
            if (!"".equals(fileName)) {
                document = reader.read(fileName);
            } else {
                //随机一个配置文件
                getRandomDocument();
            }
        } catch (DocumentException e) {
            LogManager.getLogger().error(e);
        }
    }

    public static LoadConfig getConfig() {
        return instance;
    }

    public static LoadConfig getConfig(String configFile) {
        instance=new LoadConfig(configFile);
        return instance;
    }

    //生成随机的配置文件
    private void getRandomDocument() {
        Random r = new Random();
        document = DocumentHelper.createDocument();
        Element generator = document.addElement("generator");
        Element rangeRandomCount = generator.addElement("rangeRandomCount");
        rangeRandomCount.setText("100");
        Element type = generator.addElement("type");
        type.setText("double");

        int tableNum = r.nextInt(10) + 4;
        int workNum = 3;
        ArrayList<Element> tuples = new ArrayList<>();
        for (int i = 0; i < tableNum; i++) {
            Element table = generator.addElement("table");
            Element tableSize = table.addElement("tableSize");
            tableSize.setText("100");
            int tupleNum = 3 + r.nextInt(10);
            for (int j = 0; j < tupleNum; j++) {
                Element tuple = table.addElement("tuple");
                tuples.add(tuple);
                Element min = tuple.addElement("min");
                min.setText("0");
                Element max = tuple.addElement("range");
                max.setText("100000");
                Element work = tuple.addElement("work");
                work.addAttribute("id", String.valueOf(0)).setText("inout");
            }
        }

        Element threads = generator.addElement("threads");
        Element runCount = threads.addElement("runCount");
        runCount.setText("1000");
    }

    //运行的相关信息

    public int getThreadNum() {
        return 8*Runtime.getRuntime().availableProcessors();
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
    public Double getTableSparsity(){
        return Double.valueOf(document.valueOf("//generator/table/tableSparsity"));
    }

    public String getType() {
        return document.valueOf("//generator/type");
    }

    public int getTupleNum(){
        return Integer.valueOf(document.valueOf("//generator/tuple/num"));
    }

    public double getTupleMin(){
        return Integer.valueOf(document.valueOf("//generator/tuple/min"));
    }

    public double getTupleRange(){
        return Integer.valueOf(document.valueOf("//generator/tuple/max"))-getTupleMin();
    }
}
