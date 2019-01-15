package ecnu.db;

import ecnu.db.checking.CheckCorrectness;
import ecnu.db.scheme.Table;
import ecnu.db.threads.LoadData;
import ecnu.db.threads.MyThreadFactory;
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
public class DbChecking {
    /**
     * 数据表
     */
    private Table[] tables;

    /**
     * 线程池
     */
    private ThreadPoolExecutor threadPoolExecutor;

    private DbChecking(String configFile) {
        //载入配置文件
        LoadConfig.fileName=configFile;
        //初始化数据表
        tables = new Table[LoadConfig.getConfig().getTableNum()];
        int[] tableSizes = LoadConfig.getConfig().getTableSize();
        for (int i = 0; i < tables.length; i++) {
            tables[i] = new Table(i, tableSizes[i]);
        }
        //初始化线程池
        int coreNum = Runtime.getRuntime().availableProcessors();
        int maxPoolSize = 10 * coreNum;
        long keepAliveTime = 5000;

        threadPoolExecutor = new ThreadPoolExecutor(coreNum,
                maxPoolSize,
                keepAliveTime,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(),
                new MyThreadFactory());
    }

    public static void main(String[] args) {
        //DbChecking dbChecking = new DbChecking("config/SingleTableCheckConfig.xml");
        DbChecking dbChecking = new DbChecking("");
        dbChecking.createScheme();
        dbChecking.loadData();
        dbChecking.work();
        dbChecking.closeThreadPoolExecutor();
    }

    /**
     * 重建scheme
     */
    private void createScheme() {
        MysqlConnector mysqlConnector = new MysqlConnector();
        System.out.println("数据库连接成功！");
        System.out.println("开始重建数据库scheme！");
        //删除之前的scheme
        mysqlConnector.dropTables(tables.length);
        //为每张新表重建scheme
        for (Table table : tables) {
            mysqlConnector.executeSql(table.getSQL());
        }
        System.out.println("数据库scheme重建成功！");
        mysqlConnector.close();
    }

    /**
     * 多线程导入数据
     */
    private void loadData() {
        LoadData[] loadData = new LoadData[tables.length];
        CountDownLatch count = new CountDownLatch(tables.length);
        for (int i = 0; i < tables.length; i++) {
            loadData[i] = new LoadData(tables[i], count);
            threadPoolExecutor.submit(loadData[i]);
        }
        try {
            count.await();
            System.out.println("导入数据完成！");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void work() {
        CheckCorrectness checkCorrectness = new CheckCorrectness(threadPoolExecutor);
        checkCorrectness.computeBeginSum();
        checkCorrectness.printWorkGroup();
        checkCorrectness.work(tables);
        checkCorrectness.computeEndSum();
        checkCorrectness.printWorkGroup();
        checkCorrectness.checkCorrect();
    }

    private void closeThreadPoolExecutor() {
        threadPoolExecutor.shutdown();
    }
}
