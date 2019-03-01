package ecnu.db.core;

import ecnu.db.scheme.Table;
import ecnu.db.threads.ComputeSum;
import ecnu.db.threads.LoadData;
import ecnu.db.threads.TransactionThread;
import ecnu.db.threads.pool.DbCheckingThreadPool;
import ecnu.db.transaction.*;
import ecnu.db.utils.LoadConfig;
import ecnu.db.utils.MysqlConnector;
import ecnu.db.utils.RandomTupleSize;
import org.apache.logging.log4j.LogManager;

import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.stream.IntStream;


/**
 * @author wangqingshuai
 * 核心校验类，包含前置求和 后置求和 以及事务执行的相关代码
 */
public class DbChecking {
    private ArrayList<WorkGroup> workGroups;
    private Table[] tables;
    private CheckType checkType;

    public void setCheckType(CheckType checkType) {
        this.checkType = checkType;
        workGroups=new WorkGroups(tables,checkType.getCheckKind()).getWorkGroups();
    }

    public DbChecking(){
        //初始化数据表
        tables = new Table[LoadConfig.getConfig().getTableNum()];
        int tableSizes = LoadConfig.getConfig().getTableSize();
        RandomTupleSize randomTupleSize=new RandomTupleSize(
                tables.length,LoadConfig.getConfig().getTupleNum());
        for (int i = 0; i < tables.length; i++) {
            tables[i] = new Table(i, tableSizes,randomTupleSize.getTupleSize());
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

    public void check(){
        computeSum(true);
        work();
        computeSum(false);
        if(checkCorrect()){
            System.out.println("当前隔离级别达到了"+checkType.getCheckKind());
        }else{
            System.out.println("当前隔离级别没有达到"+checkType.getCheckKind());
        }
    }

    private void work() {
        System.out.println("开始执行事务");
        int runCount = LoadConfig.getConfig().getRunCount();
        int threadsNum=LoadConfig.getConfig().getThreadNum();
        CountDownLatch count = new CountDownLatch(threadsNum);
        for (int i = 0; i < threadsNum; i++) {
            TransactionThread transactionThread=new TransactionThread(
                    tables,workGroups,checkType,runCount,count);
            DbCheckingThreadPool.getThreadPoolExecutor().submit(transactionThread);
        }
        try {
            count.await();
            System.out.println("全部事务执行完毕！");
        } catch (InterruptedException e) {
            LogManager.getLogger().error(e);
        }

    }

    public void printWorkGroup() {
        for (WorkGroup workGroup : workGroups) {
            System.out.println(workGroup);
        }
    }

    private void computeSum(Boolean isBegin) {
        ComputeSum[] computeSums = new ComputeSum[workGroups.size()];
        CountDownLatch count = new CountDownLatch(workGroups.size());
        for (int i = 0; i < workGroups.size(); i++) {
            computeSums[i] = new ComputeSum(workGroups.get(i), count, isBegin);
            DbCheckingThreadPool.getThreadPoolExecutor().submit(computeSums[i]);
        }
        try {
            count.await();
        } catch (InterruptedException e) {
            LogManager.getLogger().error(e);
        }
    }

    private boolean checkCorrect() {
        boolean checkResult=true;
        for (WorkGroup workGroup : workGroups) {
            checkResult =checkResult & workGroup.checkCorrect();
        }
        return checkResult;
    }
}
