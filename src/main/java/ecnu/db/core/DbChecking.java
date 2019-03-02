package ecnu.db.core;

import ecnu.db.scheme.Table;
import ecnu.db.threads.LoadData;
import ecnu.db.threads.TransactionThread;
import ecnu.db.threads.pool.DbCheckingThreadPool;
import ecnu.db.utils.LoadConfig;
import ecnu.db.utils.MysqlConnector;
import ecnu.db.utils.RandomTupleSize;
import ecnu.db.workGroup.BaseWorkGroup;
import ecnu.db.workGroup.InitAllWorkGroup;
import org.apache.logging.log4j.LogManager;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;


/**
 * @author wangqingshuai
 * 核心校验类，包含前置求和 后置求和 以及事务执行的相关代码
 */
public class DbChecking {
    private ArrayList<BaseWorkGroup> workGroups;
    private Table[] tables;
    private CheckType checkType;

    public DbChecking(CheckType checkType) {
        //初始化数据表
        this.checkType = checkType;
        tables = new Table[LoadConfig.getConfig().getTableNum()];
        int tableSizes = LoadConfig.getConfig().getTableSize();

        if (checkType.getCheckKind() == CheckType.CheckKind.Serializable) {
            tables[0] = new Table(0, tableSizes, 1);
            RandomTupleSize randomTupleSize = new RandomTupleSize(
                    tables.length - 1,
                    LoadConfig.getConfig().getTupleNum() - 1);
            for (int i = 1; i < tables.length; i++) {
                int colSize = randomTupleSize.getTupleSize();
                tables[i] = new Table(i, tableSizes, colSize);
            }
        } else {
            RandomTupleSize randomTupleSize = new RandomTupleSize(
                    tables.length, LoadConfig.getConfig().getTupleNum());
            for (int i = 0; i < tables.length; i++) {
                int colSize = randomTupleSize.getTupleSize();
                tables[i] = new Table(i, tableSizes, colSize);
            }
        }

    }


    /**
     * 重建scheme
     */
    public void createScheme() {
        MysqlConnector mysqlConnector = new MysqlConnector();
        System.out.println("数据库连接成功！");
        System.out.println("开始重建数据库scheme！");
        //删除之前的scheme
        try {
            mysqlConnector.dropTables(tables.length);
            //为每张新表重建scheme
            for (Table table : tables) {
                mysqlConnector.executeSql(table.getSQL());
            }
            mysqlConnector.createOrderTable();
            if (checkType.getCheckKind() == CheckType.CheckKind.Serializable) {
                mysqlConnector.createPhantomReadRecordTable();
            }
            System.out.println("数据库scheme重建成功！");
            mysqlConnector.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * 多线程导入数据
     */
    public void loadData() {
        LoadData[] loadData = new LoadData[tables.length];
        CountDownLatch count = new CountDownLatch(tables.length);
        for (int i = 0; i < tables.length; i++) {
            loadData[i] = new LoadData(tables[i], count);
            DbCheckingThreadPool.getThreadPoolExecutor().submit(loadData[i]);
        }
        try {
            count.await();
            System.out.println("导入数据完成！");
        } catch (InterruptedException e) {
            LogManager.getLogger().error(e);
        }
    }

    public void check() {
        //初始化工作组
        workGroups = new InitAllWorkGroup(tables, checkType).getWorkGroups();
        printWorkGroup();
        try {
            computeSum(true);
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println("无法记录数据初始状态");
            System.exit(-1);
        }
        work();
        try {
            computeSum(false);
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println("无法记录数据结束状态");
            System.exit(-1);
        }
        if (checkCorrect()) {
            System.out.println("当前隔离级别达到了" + checkType.getCheckKind());
        } else {
            System.out.println("当前隔离级别没有达到" + checkType.getCheckKind());
        }
    }

    private void work() {
        System.out.println("开始执行事务");
        int runCount = LoadConfig.getConfig().getRunCount();
        int threadsNum = LoadConfig.getConfig().getThreadNum();
        CountDownLatch count = new CountDownLatch(threadsNum);
        for (int i = 0; i < threadsNum; i++) {
            TransactionThread transactionThread = null;
            try {
                transactionThread = new TransactionThread(
                        tables, workGroups, checkType, runCount, count);
            } catch (SQLException e) {
                e.printStackTrace();
                System.out.println("线程组初始化失败");
                System.exit(-1);
            }
            //transactionThread.run();
            DbCheckingThreadPool.getThreadPoolExecutor().submit(transactionThread);
        }
        try {
            count.await();
            System.out.println("全部事务执行完毕！");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    private void printWorkGroup() {
        for (BaseWorkGroup workGroup : workGroups) {
            System.out.println(workGroup);
        }
    }

    private void computeSum(Boolean isBegin) throws SQLException {
        MysqlConnector mysqlConnector = new MysqlConnector();
        for (BaseWorkGroup workGroup : workGroups) {
            workGroup.computeAllSum(isBegin, mysqlConnector);
        }
        mysqlConnector.close();
    }

    private boolean checkCorrect() {
        boolean checkResult = true;
        for (BaseWorkGroup workGroup : workGroups) {
            boolean temp = workGroup.checkCorrect();
            if (!temp) {
                System.out.println(workGroup.getWorkGroupType() + "验证失败");
            }
            checkResult = checkResult & temp;
        }
        return checkResult;
    }
}
