package ecnu.db.threads;

import ecnu.db.scheme.DoubleTuple;
import ecnu.db.utils.DataInputFromFile;
import ecnu.db.utils.LoadConfig;
import ecnu.db.utils.MysqlConnector;
import ecnu.db.utils.ReadLogs;
import org.apache.commons.lang3.mutable.MutableInt;

import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 * @author wangqingshuai
 * 检测order中每列的结果是否正确的线程
 */
public class OrderChecking implements Runnable {
    private int tableIndex;
    private int tupleIndex;
    private CountDownLatch count;
    private boolean add;

    public OrderChecking(boolean add, int tableIndex, int tupleIndex, CountDownLatch count) {
        this.add = add;
        this.tableIndex = tableIndex;
        this.tupleIndex = tupleIndex;
        this.count = count;
    }

    @Override
    public void run() {
        DataInputFromFile dataInputFromFile = new DataInputFromFile(tableIndex);
        double[] results = dataInputFromFile.readData(tupleIndex);
        Map<Integer, MutableInt> ops = ReadLogs.getInstance().getDatas(tableIndex, tupleIndex);
        if (ops != null) {
            if (add) {
                for (Map.Entry<Integer, MutableInt> entry : ops.entrySet()) {
                    results[entry.getKey() - entry.getKey() / LoadConfig.getConfig().getKeyRange() - 1] += entry.getValue().getValue();
                }
            } else {
                for (Map.Entry<Integer, MutableInt> entry : ops.entrySet()) {
                    results[entry.getKey()] -= entry.getValue().getValue();
                }
            }

        }
        MysqlConnector mysqlConnector = new MysqlConnector();
        Double[] dataBaseData = mysqlConnector.getTableData(tableIndex, tupleIndex);

        for (int i = 0; i < results.length; i++) {
            if (!DoubleTuple.df.format(results[i]).equals(DoubleTuple.df.format(dataBaseData[i]))) {
                System.out.println("第" + tableIndex + "张表，第" + tupleIndex + "列校验完成，校验结果为");
                System.out.println("不匹配，在第" + i + "行，本地计算数据为" + results[i]
                        + "在线数据为" + dataBaseData[i]);
                count.countDown();
                return;
            }
        }
        System.out.println("第" + tableIndex + "张表，第" + tupleIndex + "列校验完成，结果完全相同");
        count.countDown();
        mysqlConnector.close();
    }
}
