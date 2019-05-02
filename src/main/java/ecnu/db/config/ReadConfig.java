package ecnu.db.config;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Node;
import org.dom4j.io.SAXReader;

import java.util.List;
import java.util.Random;

class ReadConfig {
    static final Random R = new Random();
    Document document;

    ReadConfig(String fileName) {
        try {
            SAXReader reader = new SAXReader();
            document = reader.read(fileName);
        } catch (DocumentException e) {
            e.printStackTrace();
        }
    }

    int getValueFromHistogram(String histogramName) throws Exception {
        double radio = R.nextDouble();
        List<Node> nodeList = document.selectNodes(histogramName + "/HistogramItem");
        double old = 0;
        for (Node node : nodeList) {
            old += Double.valueOf(node.valueOf("ratio"));
            if (radio < old) {
                int min = Integer.valueOf(node.valueOf("minValue"));
                int max = Integer.valueOf(node.valueOf("maxValue"));
                return min + R.nextInt(max - min + 1);
            }
        }
        throw new Exception("配置文件中" + histogramName + "的概率和小于1,或者不存在该配置项");
    }
}
