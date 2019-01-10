package ecnu.db;

import ecnu.db.scheme.Table;
import ecnu.db.threads.MyThreadFactory;
import ecnu.db.threads.LoadData;
import ecnu.db.utils.LoadConfig;
import ecnu.db.utils.MysqlConnector;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author wangqingshuai
 * 项目执行的主函数
 */
public class Main {

    public static void main(String[] args) {
        //表数量
        int tableNum = LoadConfig.getConfig().getTableNum();

        //每张表需要的行数
        int[] tableSizes = LoadConfig.getConfig().getTableSize();

        //数据表
        Table[] tables = new Table[tableNum];

        //重建scheme
        MysqlConnector mysqlConnector = new MysqlConnector();
        System.out.println("数据库连接成功！");
        System.out.println("开始重建数据库scheme！");
        mysqlConnector.dropTables(tableNum);
        for (int i = 0; i < tableNum; i++) {
            tables[i] = new Table(i, tableSizes[i]);
            mysqlConnector.excuteSql(tables[i].getSQL());
        }
        System.out.println("数据库scheme重建成功！");
        //初始化线程池
        int coreNum = Runtime.getRuntime().availableProcessors();
        int maxPoolSize = 4 * coreNum;
        long keepAliveTime = 5000;

        ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(coreNum,
                maxPoolSize,
                keepAliveTime,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(),
                new MyThreadFactory());

        //多线程导入数据
        LoadData[] loadData = new LoadData[tableNum];
        CountDownLatch count = new CountDownLatch(tableNum);
        for (int i = 0; i < tableNum; i++) {
            loadData[i] = new LoadData(tables[i], count);
            threadPoolExecutor.submit(loadData[i]);
        }
        try {
            count.await();
            System.out.println("导入数据完成！");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        threadPoolExecutor.shutdown();
    }
}
