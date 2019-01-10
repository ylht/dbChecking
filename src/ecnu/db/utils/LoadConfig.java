package ecnu.db.utils;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Node;
import org.dom4j.io.SAXReader;

import java.util.List;

/**
 * @author wangqingshuai
 * 读取配置文件的类
 */
public class LoadConfig {
    private static LoadConfig instance;
    private Document document;

    private LoadConfig() {
        try {
            SAXReader reader = new SAXReader();
            document = reader.read("config/SingelTableCheckConfig.xml");
        } catch (DocumentException e) {
            e.printStackTrace();
        }
    }

    public synchronized static LoadConfig getConfig() {
        if (instance == null) {
            instance = new LoadConfig();
        }
        return instance;
    }


    public Integer getRangeRandomCount() {
        return Integer.valueOf(document.valueOf("//generator/rangeRandomCount"));
    }

    public int getTableNum() {
        return document.selectNodes("//generator/table").size();
    }

    public List<Node> getTableTupleInfo(int tableIndex) {
        String xpath = "//generator/table";
        List<Node> list = document.selectNodes(xpath);
        Node table = list.get(tableIndex);
        return table.selectNodes("tuple");
    }

    public int[] getTableSize() {
        String xpath = "//generator/table";
        List<Node> list = document.selectNodes(xpath);
        int[] tableSizes = new int[list.size()];
        int i = 0;
        for (Node l : list) {
            tableSizes[i++] = Integer.valueOf(l.valueOf("tableSize"));
        }
        return tableSizes;
    }

}
