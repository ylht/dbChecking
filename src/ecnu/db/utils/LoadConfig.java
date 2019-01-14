package ecnu.db.utils;

import ecnu.db.checking.WorkGroup;
import ecnu.db.checking.WorkNode;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Node;
import org.dom4j.io.SAXReader;

import java.util.ArrayList;
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

    public static void main(String[] args) {
        System.out.println(LoadConfig.getConfig().getThreadNum(0));
    }

    public int getRunCount() {
        String xpath = "//generator/threads/runCount";
        Node list = document.selectNodes(xpath).get(0);
        return Integer.valueOf(list.getText());
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

    public int getThreadNum(int workGroupNum) {
        String xpath = "//generator/threads/work[@id='" + workGroupNum + "']";
        Node list = document.selectNodes(xpath).get(0);
        return Integer.valueOf(list.getText());
    }

    public ArrayList<WorkGroup> getWorkNode() {
        ArrayList<WorkGroup> resultNodes = new ArrayList<>();
        String xpath = "//generator/table";
        List<Node> list = document.selectNodes(xpath);
        int tableIndex = 0;
        for (Node table : list) {
            List<Node> tupleList = table.selectNodes("tuple");
            int tupleIndex = 1;
            for (Node tuple : tupleList) {
                if (!"".equals(tuple.valueOf("work"))) {
                    int workId = Integer.valueOf(tuple.valueOf("work/@id"));
                    boolean hasWorkGroup = false;
                    for (WorkGroup workGroup : resultNodes) {
                        if (workGroup.getWorkId() == workId) {
                            switch (tuple.valueOf("work")) {
                                case "in":
                                    workGroup.addInTuple(new WorkNode(tableIndex, tupleIndex));
                                    break;
                                case "out":
                                    workGroup.addOutTuple(new WorkNode(tableIndex, tupleIndex));
                                    break;
                                case "inout":
                                    workGroup.addInoutTuple(new WorkNode(tableIndex, tupleIndex));
                                    break;
                                default:
                                    System.out.println("没有匹配到work类型，请检查配置文件");
                                    System.exit(-1);
                            }
                            hasWorkGroup = true;
                        }
                    }
                    if (!hasWorkGroup) {
                        resultNodes.add(new WorkGroup(workId));
                        switch (tuple.valueOf("work")) {
                            case "in":
                                resultNodes.get(resultNodes.size() - 1)
                                        .addInTuple(new WorkNode(tableIndex, tupleIndex));
                                break;
                            case "out":
                                resultNodes.get(resultNodes.size() - 1)
                                        .addOutTuple(new WorkNode(tableIndex, tupleIndex));
                                break;
                            case "inout":
                                resultNodes.get(resultNodes.size() - 1)
                                        .addInoutTuple(new WorkNode(tableIndex, tupleIndex));
                                break;
                            default:
                                System.out.println("没有匹配到work类型，请检查配置文件");
                                System.exit(-1);
                        }
                    }
                }
                tupleIndex++;
            }
            tableIndex++;
        }
        return resultNodes;
    }
}
