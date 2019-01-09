package ecnu.db.utils;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Node;
import org.dom4j.io.SAXReader;

import java.util.ArrayList;
import java.util.List;

public class LoadConfig {
    private Document document;

    private LoadConfig(){
        try {
            SAXReader reader = new SAXReader();
            document = reader.read("config/SingelTableCheckConfig.xml");
        } catch (DocumentException e) {
            e.printStackTrace();
        }
    }

    private static LoadConfig instance;
    public synchronized static LoadConfig getConfig() {
        if (instance == null) {
            instance = new LoadConfig();
        }
        return instance;
    }


    public Integer getRangeRandomCount() {
        return Integer.valueOf(document.valueOf("//generator/table/rangeRandomCount"));
    }

    public List<Node> getTableIntTupleInfo(int tableIndex){
        String xpath = "//generator/table";
        List<Node> list = document.selectNodes(xpath);
        Node table=list.get(tableIndex);
        return table.selectNodes("tuple");
    }

//    public static void main(String[] args) {
//        Document dc = LoadConfig.getConfig();
//        String xpath = "//generator/table/scheme";
//        List<Node> list = dc.selectNodes(xpath);
//        for (Node l : list) {
//            System.out.println(l.valueOf("min"));
//        }
//    }
}
