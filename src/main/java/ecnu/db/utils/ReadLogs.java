package ecnu.db.utils;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.mutable.MutableInt;

import java.io.FileReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

/**
 * @author wangqingshuai
 * 从order日志中读取操作记录
 */
public class ReadLogs {
    private static ReadLogs instance;
    private Map<Vector<Integer>, Map<Integer, MutableInt>> counter;

    private ReadLogs() {
        try {
            FileReader fileReader = new FileReader("logs/order.log");
            CSVFormat csvFileFormat = CSVFormat.DEFAULT.withSkipHeaderRecord();
            CSVParser csvParser = new CSVParser(fileReader, csvFileFormat);
            counter = new HashMap<>();
            List<CSVRecord> csvRecords = csvParser.getRecords();
            for (CSVRecord csvRecord : csvRecords) {

                Vector<Integer> key = new Vector<>();
                key.add(Integer.parseInt(csvRecord.get(0)));
                key.add(Integer.parseInt(csvRecord.get(1)));

                Map<Integer, MutableInt> valueList = counter.get(key);
                if (valueList == null) {
                    valueList = new HashMap<>();
                    MutableInt newValue = new MutableInt(1);
                    valueList.put(Integer.parseInt(csvRecord.get(2)), newValue);
                    counter.put(key, valueList);
                } else {
                    MutableInt newValue = new MutableInt(1);
                    MutableInt oldValue = valueList.put(Integer.parseInt(csvRecord.get(2)), newValue);
                    if (oldValue != null) {
                        newValue.setValue(oldValue.getValue() + 1);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public synchronized static ReadLogs getInstance() {
        if (instance == null) {
            instance = new ReadLogs();
        }
        return instance;
    }

    public static void main(String[] args) {
        for (Map.Entry<Integer, MutableInt> entry : ReadLogs.getInstance().getDatas(1, 3).entrySet()) {
            System.out.println(entry.getKey() + " " + entry.getValue());
        }
    }

    public Map<Integer, MutableInt> getDatas(int tableIndex, int tupleIndex) {
        Vector<Integer> key = new Vector<>();
        key.add(tableIndex);
        key.add(tupleIndex);
        return counter.get(key);
    }
}
