package ecnu.db.threads;

import ecnu.db.utils.DataInputFromFile;
import ecnu.db.utils.MysqlConnector;
import ecnu.db.utils.ReadLogs;
import org.apache.commons.lang3.mutable.MutableInt;

import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class OrderChecking implements Runnable {
    private int tableIndex;
    private int tupleIndex;
    private CountDownLatch count;

    public OrderChecking(int tableIndex, int tupleIndex, CountDownLatch count) {
        this.tableIndex = tableIndex;
        this.tupleIndex = tupleIndex;
        this.count = count;
    }

    @Override
    public void run() {
        DataInputFromFile dataInputFromFile = new DataInputFromFile(tableIndex);
        double[] results = dataInputFromFile.readData(tupleIndex);
        Map<Integer, MutableInt> ops = ReadLogs.getInstance().getDatas(tableIndex, tupleIndex);
        for (Map.Entry<Integer, MutableInt> entry : ops.entrySet()) {
            results[entry.getKey()] += entry.getValue().getValue();
        }
        MysqlConnector mysqlConnector = new MysqlConnector();
        Double[] dataBaseData = mysqlConnector.getTableData(tableIndex, tupleIndex);
        for (int i = 0; i < results.length; i++) {
            if (results[i] != dataBaseData[i]) {
                System.out.println("Order数据不匹配，在第" + i + "行，本地计算数据为" + results[i]
                        + "在线数据为" + dataBaseData[i]);
                count.countDown();
                return;
            }
        }
        System.out.println("第" + tableIndex + "张表,第" + tupleIndex + "个数据计算结果完全匹配");
        count.countDown();
    }
}