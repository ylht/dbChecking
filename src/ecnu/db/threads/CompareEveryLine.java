package ecnu.db.threads;

import ecnu.db.scheme.DoubleTuple;
import ecnu.db.utils.DataInputFromFile;
import ecnu.db.utils.MysqlConnector;

import java.io.IOException;

public class CompareEveryLine implements Runnable {
    private int tableIndex;
    private double[][] tableData;

    public CompareEveryLine(int tableIndex, double[][] tableData) {
        this.tableIndex = tableIndex;
        this.tableData = tableData;
    }

    @Override
    public void run() {
        DataInputFromFile dataInputFromFile = new DataInputFromFile(tableIndex);
        try {
            double[][] beginData = dataInputFromFile.readData();
            for (int i = 0; i < tableData.length; i++) {
                for (int j = 0; j < tableData[0].length; j++) {
                    tableData[i][j] += beginData[i][j];
                }
            }
            MysqlConnector mysqlConnector = new MysqlConnector();
            double[][] onlineData = mysqlConnector.getTableData(tableIndex);
            if (compareData(tableData, onlineData)) {
                System.out.println("表" + tableIndex + "数据执行结果正确");
            } else {
                System.out.println("表" + tableIndex + "数据执行结果不正确");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private boolean compareData(double[][] preData, double[][] posData) {
        if (preData.length != posData.length || preData[0].length != posData[0].length) {
            try {
                throw new Exception("本地数据集和在线数据集大小不匹配");
            } catch (Exception e) {
                e.printStackTrace();
                return false;
            }
        } else {
            for (int i = 0; i < posData.length; i++) {
                for (int j = 0; j < posData[0].length; j++) {
                    if (!Double.valueOf(DoubleTuple.df.format(preData[i][j])).equals(posData[i][j])) {
                        return false;
                    }
                }
            }
            return true;
        }
    }
}
