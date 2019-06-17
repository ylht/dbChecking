package ecnu.db;

import com.google.common.reflect.ClassPath;
import ecnu.db.check.BaseCheck;
import ecnu.db.check.CheckNode;
import ecnu.db.config.SystemConfig;
import ecnu.db.config.TableConfig;
import ecnu.db.scheme.AbstractColumn;
import ecnu.db.scheme.Table;
import ecnu.db.threads.LoadData;
import ecnu.db.threads.TransactionThread;
import ecnu.db.threads.pool.DbCheckingThreadPool;
import ecnu.db.transaction.BaseTransaction;
import ecnu.db.utils.MysqlConnector;
import org.apache.logging.log4j.LogManager;
import org.checkerframework.checker.units.qual.A;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;
import java.util.concurrent.CountDownLatch;


/**
 * @author wangqingshuai
 * 核心校验类，包含前置求和 后置求和 以及事务执行的相关代码
 */
public class DbChecking {
    private ArrayList<BaseCheck> checkGroups;
    private Table[] tables;
    private BaseCheck.CheckKind checkKind;

    DbChecking(BaseCheck.CheckKind checkKind) {
        //初始化数据表
        this.checkKind = checkKind;
        try {
            tables = new Table[TableConfig.getConfig().getTableNum()];
            ArrayList<ArrayList<Integer>> allKeys = new ArrayList<>();
            for (int i = 0; i < tables.length; i++) {
                tables[i] = new Table(i, allKeys);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 重建scheme
     */
    void createScheme() {
        MysqlConnector mysqlConnector = new MysqlConnector();
        System.out.println("数据库连接成功！");
        System.out.println("开始重建数据库scheme！");
        //删除之前的scheme
        try {
            mysqlConnector.dropTables();
            //为每张新表重建scheme
            for (Table table : tables) {
                mysqlConnector.executeSql(table.getSQL());
            }
            mysqlConnector.createOrderTable();
            if (checkKind == BaseCheck.CheckKind.Serializable) {
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
    void loadData() {
        System.out.println("开始导入数据");
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

    /**
     * 根据配置文件和当前的隔离级别判定需要加载运行事务类
     *
     * @return 需要检测的事务
     * @throws Exception 无法初始化事务
     */
    private ArrayList<BaseCheck> getChecks() throws Exception {
        ArrayList<BaseCheck> checkGroups = new ArrayList<>();
        final ClassLoader loader = Thread.currentThread().getContextClassLoader();
        for (final ClassPath.ClassInfo info : ClassPath.from(loader).getTopLevelClasses()) {
            if (info.getName().startsWith("ecnu.db.check.groups")) {
                BaseCheck checkGroup = (BaseCheck) info.load().getConstructor().newInstance();
                if (checkGroup.checkOrNot(checkKind)) {
                    checkGroups.add(checkGroup);
                }
            }
        }
        return checkGroups;
    }


    private HashMap<AbstractColumn.ColumnType, ArrayList<CheckNode>> getAllWorkNodes() {
        HashMap<AbstractColumn.ColumnType, ArrayList<CheckNode>> checkNodes =
                new HashMap<>(AbstractColumn.ColumnType.values().length);
        for (int i = 0; i < AbstractColumn.ColumnType.values().length; i++) {
            checkNodes.put(AbstractColumn.ColumnType.values()[i], new ArrayList<>());
        }
        for (Table table : tables) {
            if (table.getCheckNodes() != null) {
                for (AbstractColumn.ColumnType value : AbstractColumn.ColumnType.values()) {
                    try {
                        checkNodes.get(value).addAll(table.getCheckNodes().get(value));
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        return checkNodes;
    }

    private ArrayList<BaseCheck> getCheckGroupsOnWholeTable() {
        ArrayList<BaseCheck> checks = new ArrayList<>();
        //所有被参照表的index
        HashSet<Integer> tableRefIndex = new HashSet<>();
        for (Table table : tables) {
            if (table.getForeignKeys() != null) {
                tableRefIndex.addAll(table.getForeignKeys());
            }
        }
        //没有被任何表参照的index
        ArrayList<Integer> tableNotRef = new ArrayList<>();
        for (int i = 0; i < tables.length; i++) {
            tableNotRef.add(i);
        }
        tableNotRef.removeAll(tableRefIndex);

        for (BaseCheck workGroup : checkGroups) {
            if (workGroup.getWholeTable()) {
                //获取该工作组需要的列数量
                int columnCount = workGroup.getColumnCount();
                //获取该工作组需要的列类型
                AbstractColumn.ColumnType columnType = workGroup.columnType();
                int getIndex = new Random().nextInt(tableNotRef.size());
                int tableIndex = tableNotRef.get(getIndex);
                addCheckGroup(workGroup, columnCount, columnType, tableIndex);
                tables[tableIndex].setNoCheckNodes();
                if (workGroup.columnNumNotEnough()) {
                    System.out.println("移除" + workGroup.getClass().getSimpleName());
                } else {
                    checks.add(workGroup);
                }
                tableNotRef.remove(getIndex);
                if (tableNotRef.size() == 0) {
                    break;
                }
            }

        }
        return checks;
    }

    private ArrayList<BaseCheck> getCheckGroupsOnSameTable() {
        ArrayList<BaseCheck> checks = new ArrayList<>();
        ArrayList<Integer> allIndex = new ArrayList<>();
        for (int i = 0; i < tables.length; i++) {
            if(tables[i].getCheckNodes()!=null){
                allIndex.add(i);
            }
        }
        for (BaseCheck workGroup : checkGroups) {
            if (workGroup.getColumnFromSameTable() && !workGroup.getWholeTable()) {
                int columnCount = workGroup.getColumnCount();
                AbstractColumn.ColumnType columnType=workGroup.columnType();

                int tableIndex = allIndex.get(new Random().nextInt(allIndex.size()));
                addCheckGroup(workGroup, columnCount, columnType, tableIndex);
                if (workGroup.columnNumNotEnough()) {
                    System.out.println("移除" + workGroup.getClass().getSimpleName());
                } else {
                    checks.add(workGroup);
                }
            }
        }
        return checks;
    }

    private ArrayList<BaseCheck> getNormalCheckGroups() {
        ArrayList<BaseCheck> checks = new ArrayList<>();
        HashMap<AbstractColumn.ColumnType, ArrayList<CheckNode>> newWorksNodes = getAllWorkNodes();
        for (BaseCheck workGroup : checkGroups) {
            if (workGroup.getColumnFromSameTable()||workGroup.getWholeTable()) {
                continue;
            }
            int columnCount  = workGroup.getColumnCount();
            AbstractColumn.ColumnType columnType= workGroup.columnType();
            for (int i = 0; i < columnCount; i++) {
                try {
                    workGroup.addWorkNode(newWorksNodes.get(columnType).remove(0));
                } catch (Exception e) {
                    System.out.println(workGroup.getClass().getSimpleName() + "没有workNode可供选择");
                    break;
                }
            }
            if (workGroup.columnNumNotEnough()) {
                System.out.println("移除" + workGroup.getClass().getSimpleName());
            } else {
                checks.add(workGroup);
            }
        }
        return checks;
    }


    private void addCheckGroup(BaseCheck workGroup, int columnCount, AbstractColumn.ColumnType columnType, int tableIndex) {
        for (int i = 0; i < columnCount; i++) {
            try {
                ArrayList<CheckNode> nodes = tables[tableIndex].getCheckNodes().get(columnType);
                workGroup.addWorkNode(nodes.remove(0));
            } catch (Exception e) {
                System.out.println(workGroup.getClass().getSimpleName() + "没有workNode可供选择");
                break;
            }
        }
    }

    public void check() {
        //初始化需要做的工作组
        try {
            checkGroups = getChecks();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("无法初始化检测组");
            System.out.println(-1);
        }
        ArrayList<BaseCheck> checks=new ArrayList<>();
        //可以运行的工作组添加到checks中
        checks.addAll(getCheckGroupsOnWholeTable());
        checks.addAll(getCheckGroupsOnSameTable());
        checks.addAll(getNormalCheckGroups());
        checkGroups=checks;
        try {
            recordStartStatus();
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println("无法记录数据初始状态");
            System.exit(-1);
        }
        printWorkGroup();
        work();
        try {
            recordEndStatus();
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println("无法记录数据结束状态");
            System.exit(-1);
        }
        printWorkGroup();
        if (checkCorrect()) {
            System.out.println("当前隔离级别达到了" + checkKind);
        } else {
            System.out.println("当前隔离级别没有达到" + checkKind);
        }
        //检测完毕关闭所有线程组
        DbCheckingThreadPool.closeThreadPool();
    }

    private void work() {
        System.out.println("开始执行事务");
        int runCount = SystemConfig.getConfig().getRunCount();
        int threadsNum = SystemConfig.getConfig().getThreadNum();
        CountDownLatch count = new CountDownLatch(threadsNum);
        ArrayList<BaseTransaction> transactions = new ArrayList<>();
        for (BaseCheck workGroup : checkGroups) {
            workGroup.makeTransaction();
            try {
                transactions.add(workGroup.getTransaction());
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("没有为" + workGroup.getClass().getSimpleName() + "设置事务");
            }
        }
        for (int i = 0; i < threadsNum; i++) {
            TransactionThread transactionThread = new TransactionThread(i, transactions, runCount, count);
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
        for (BaseCheck checkGroup : checkGroups) {
            System.out.println(checkGroup);
        }
    }

    private void recordStartStatus() throws SQLException {
        MysqlConnector mysqlConnector = new MysqlConnector();
        for (BaseCheck checkGroup : checkGroups) {
            checkGroup.recordBeginStatus(mysqlConnector);
        }
        mysqlConnector.close();
    }


    private void recordEndStatus() throws SQLException {
        MysqlConnector mysqlConnector = new MysqlConnector();
        for (BaseCheck checkGroup : checkGroups) {
            checkGroup.recordEndStatus(mysqlConnector);
        }
        mysqlConnector.close();
    }

    private boolean checkCorrect() {
        boolean checkResult = true;
        for (BaseCheck checkGroup : checkGroups) {
            boolean temp = checkGroup.checkCorrect();
            if (!temp) {
                System.out.println(checkGroup.getClass().getSimpleName() + "验证失败");
            }
            checkResult = checkResult & temp;
        }
        return checkResult;
    }
}
