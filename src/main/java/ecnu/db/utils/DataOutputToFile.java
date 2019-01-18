package ecnu.db.utils;

import java.io.FileWriter;
import java.io.IOException;

/**
 * @author wangqingshuai
 * 将生成后的数据写入到本地文件的类
 */
public class DataOutputToFile {
    private FileWriter file;

    public DataOutputToFile(int tableIndex) {
        try {
            file = new FileWriter("randomData/t" + tableIndex);
        } catch (Exception e) {
            System.out.println("写入文件randomData/t" + tableIndex + "路径失败");
        }
    }

    public void close() {
        try {
            file.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void write(Object[] dataObjects) {
        StringBuilder line = new StringBuilder();
        for (Object data : dataObjects) {
            line.append(data.toString()).append(',');
        }
        try {
            file.write(line.substring(0, line.length() - 1) + "\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}