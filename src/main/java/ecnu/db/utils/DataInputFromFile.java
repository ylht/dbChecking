package ecnu.db.utils;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.logging.log4j.LogManager;

import java.io.FileReader;
import java.io.IOException;
import java.util.List;

/**
 * @author wangqingshuai
 * 从本地记录中读取数据，用于和日志记录结合之后判定和在线数据的结果是否一致
 */
public class DataInputFromFile {
    private FileReader fileReader;
    private CSVParser csvParser;


    public DataInputFromFile(int tableIndex) {
        try {
            fileReader = new FileReader("data/t" + tableIndex);
            CSVFormat csvFileFormat = CSVFormat.DEFAULT.withSkipHeaderRecord();
            csvParser = new CSVParser(fileReader, csvFileFormat);
        } catch (Exception e) {
            System.out.println("读取文件data/t" + tableIndex + "失败");
        }
    }

    public static void main(String[] args) {
        DataInputFromFile dataInputFromFile = new DataInputFromFile(0);
        dataInputFromFile.readData(1);
    }

    public double[] readData(int tupleIndex) {
        try {
            List<CSVRecord> csvRecords = csvParser.getRecords();
            int lineNum = csvRecords.size();
            double[] results = new double[lineNum];
            int i = 0;
            for (CSVRecord csvRecord : csvRecords) {
                results[i++] = Double.valueOf(csvRecord.get(tupleIndex));
            }
            fileReader.close();
            return results;
        } catch (IOException e) {
            LogManager.getLogger().error(e);
            return null;
        }
    }
}
