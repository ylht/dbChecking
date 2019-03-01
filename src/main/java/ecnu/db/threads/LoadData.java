package ecnu.db.threads;

import ecnu.db.scheme.Table;
import ecnu.db.utils.DataOutputToFile;
import ecnu.db.utils.MysqlConnector;

import java.sql.SQLException;
import java.util.concurrent.CountDownLatch;

/**
 * @author wangqingshuai
 * 将数据生成到本地之后，调用mysql的命令将数据上传的服务器
 */
public class LoadData implements Runnable {
    private Table table;
    private CountDownLatch count;

    public LoadData(Table table, CountDownLatch count) {
        this.table = table;
        this.count = count;
    }

    @Override
    public void run() {
        System.out.println("表" + table.getTableIndex() + "开始生成数据到本地文件");
        DataOutputToFile df = new DataOutputToFile(table.getTableIndex());
        while (true) {
            Object[] record = table.getValue();
            if (record == null) {
                break;
            }
            df.write(record);
        }
        df.close();
        System.out.println("表" + table.getTableIndex() + "生成数据完成");
        System.out.println("表" + table.getTableIndex() + "开始上传数据到数据库");
        MysqlConnector mysqlConnector = new MysqlConnector();
        try {
            mysqlConnector.loadData(table.getTableIndex());
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println("数据上传失败");
            System.exit(-1);
        }
        System.out.println("表" + table.getTableIndex() + "上传数据完成");
        count.countDown();
    }
}
