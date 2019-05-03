package ecnu.db;

import com.google.common.reflect.ClassPath;
import ecnu.db.check.BaseCheck;
import ecnu.db.check.WorkNode;
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
import java.util.*;
import java.util.concurrent.CountDownLatch;


/**
 * @author wangqingshuai
 * 核心校验类，包含前置求和 后置求和 以及事务执行的相关代码
 */
public class DbChecking {
    private ArrayList<BaseCheck> workGroups;
    private Table[] tables;
    private BaseCheck.CheckKind checkKind;

    public DbChecking(BaseCheck.CheckKind checkKind) {
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
    public void createScheme() {
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
    public void loadData() {
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

    private ArrayList<BaseCheck> getChecks() throws Exception {
        ArrayList<BaseCheck> checkCorrectnesses = new ArrayList<>();
        final ClassLoader loader = Thread.currentThread().getContextClassLoader();
        for (final ClassPath.ClassInfo info : ClassPath.from(loader).getTopLevelClasses()) {
            if (info.getName().startsWith("ecnu.db.check.group")) {
                BaseCheck checkCorrectness = (BaseCheck) info
                        .load().getConstructor().newInstance();
                if (checkCorrectness.checkOrNot(checkKind)) {
                    checkCorrectnesses.add(checkCorrectness);
                }
            }
        }
        return checkCorrectnesses;
    }

    private HashMap<AbstractColumn.ColumnType, ArrayList<WorkNode>> initWorkNodes() {
        HashMap<AbstractColumn.ColumnType, ArrayList<WorkNode>> workNodes = new HashMap<>(AbstractColumn.ColumnType.values().length);
        for (int i = 0; i < AbstractColumn.ColumnType.values().length; i++) {
            workNodes.put(AbstractColumn.ColumnType.values()[i], new ArrayList<>());
        }
        return workNodes;
    }

    private HashMap<Integer, HashMap<AbstractColumn.ColumnType, ArrayList<WorkNode>>> getWorkNodes(Table[] tables) {
        HashMap<Integer, HashMap<AbstractColumn.ColumnType, ArrayList<WorkNode>>> workNodes = new HashMap<>(tables.length);
        for (int i = 0; i < tables.length; i++) {
            workNodes.put(i, initWorkNodes());
        }
        int tableIndex = 0;
        for (Table table : tables) {
            int foreignKeyNum = table.getForeignKeyNum();
            ArrayList<Integer> keys = table.getKeys();
            ArrayList<AbstractColumn> columns = table.getColumns();
            int columnIndex = 1;
            for (AbstractColumn column : columns) {
                if (columnIndex > foreignKeyNum) {
                    ArrayList<WorkNode>nodes=workNodes.get(tableIndex).get(column.getColumnType());
                    nodes.add(new WorkNode(tableIndex, columnIndex, keys, column.getRange()));
                }
                columnIndex++;
            }
            tableIndex++;
        }
        for (int i = 0; i < tables.length; i++) {
            for (AbstractColumn.ColumnType value : AbstractColumn.ColumnType.values()) {
                Collections.shuffle(workNodes.get(i).get(value));
            }
        }
        return workNodes;
    }

    private HashMap<AbstractColumn.ColumnType, ArrayList<WorkNode>> changeWorkNodeGroup(
            HashMap<Integer, HashMap<AbstractColumn.ColumnType, ArrayList<WorkNode>>> workNodes){
        HashMap<AbstractColumn.ColumnType, ArrayList<WorkNode>> nodes=initWorkNodes();
        for (Integer integer : workNodes.keySet()) {
            for (AbstractColumn.ColumnType value : AbstractColumn.ColumnType.values()) {
                try {
                    nodes.get(value).addAll(workNodes.get(integer).get(value));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        return nodes;
    }


    public void check() {
        //初始化需要做的工作组
        try {
            workGroups = getChecks();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("无法初始化工作组");
            System.out.println(-1);
        }
        HashMap<Integer, HashMap<AbstractColumn.ColumnType, ArrayList<WorkNode>>> workNodes = getWorkNodes(tables);
        Random r = new Random();
        //可以运行的工作组添加到checks中
        ArrayList<BaseCheck> checks=new ArrayList<>();

        for (BaseCheck workGroup : workGroups) {
            if (workGroup.getColumnFromSameTable() && workGroup.getWholeTable()) {
                int columnCount = 0;
                try {
                    columnCount = workGroup.getColumnCount();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                AbstractColumn.ColumnType columnType;
                try {
                    columnType = workGroup.columnType();
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println("无法匹配当前工作组需要的列类型");
                    continue;
                }
                int tableIndex = workNodes.size()-1;
                for (int i = 0; i < columnCount; i++) {
                    try {
                        ArrayList<WorkNode> nodes = workNodes.get(tableIndex).get(columnType);
                        workGroup.addWorkNode(nodes.remove(0));
                    } catch (Exception e) {
                        System.out.println(workGroup.getClass().getSimpleName() + "没有workNode可供选择");
                        break;
                    }
                }
                workNodes.remove(tableIndex);
                if (!workGroup.columnNumEnough()) {
                    System.out.println("移除" + workGroup.getClass().getSimpleName());
                }else{
                    checks.add(workGroup);
                }
            }

        }

        for (BaseCheck workGroup : workGroups) {
            if (workGroup.getColumnFromSameTable()&&!workGroup.getWholeTable()) {
                int columnCount = 0;
                try {
                    columnCount = workGroup.getColumnCount();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                AbstractColumn.ColumnType columnType;
                try {
                    columnType = workGroup.columnType();
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println("无法匹配当前工作组需要的列类型");
                    continue;
                }
                int tableIndex = r.nextInt(workNodes.size());
                for (int i = 0; i < columnCount; i++) {
                    try {
                        ArrayList<WorkNode> nodes = workNodes.get(tableIndex).get(columnType);
                        workGroup.addWorkNode(nodes.remove(0));
                    } catch (Exception e) {
                        System.out.println(workGroup.getClass().getSimpleName() + "没有workNode可供选择");
                        break;
                    }
                }
                if (!workGroup.columnNumEnough()) {
                    System.out.println("移除" + workGroup.getClass().getSimpleName());
                }else{
                    checks.add(workGroup);
                }
            }
        }

        HashMap<AbstractColumn.ColumnType, ArrayList<WorkNode>> newWorksNodes=changeWorkNodeGroup(workNodes);

        for (BaseCheck workGroup : workGroups) {
            if(workGroup.getColumnFromSameTable()){
                continue;
            }
            int columnCount = 0;
            try {
                columnCount = workGroup.getColumnCount();
            } catch (Exception e) {
                e.printStackTrace();
            }
            AbstractColumn.ColumnType columnType;
            try {
                columnType = workGroup.columnType();
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("无法匹配当前工作组需要的列类型");
                continue;
            }
            for (int i = 0; i < columnCount; i++) {
                try {
                    workGroup.addWorkNode(newWorksNodes.get(columnType).remove(0));
                } catch (Exception e) {
                    System.out.println(workGroup.getClass().getSimpleName() + "没有workNode可供选择");
                    break;
                }
            }
            if (!workGroup.columnNumEnough()) {
                System.out.println("移除" + workGroup.getClass().getSimpleName());
            }else{
                checks.add(workGroup);
            }
        }
        workGroups=checks;
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
        for (BaseCheck workGroup : workGroups) {
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
        for (BaseCheck workGroup : workGroups) {
            System.out.println(workGroup);
        }
    }

    private void recordStartStatus() throws SQLException {
        MysqlConnector mysqlConnector = new MysqlConnector();
        for (BaseCheck workGroup : workGroups) {
            workGroup.recordBeginStatus(mysqlConnector);
        }
        mysqlConnector.close();
    }


    private void recordEndStatus() throws SQLException {
        MysqlConnector mysqlConnector = new MysqlConnector();
        for (BaseCheck workGroup : workGroups) {
            workGroup.recordEndStatus(mysqlConnector);
        }
        mysqlConnector.close();
    }

    private boolean checkCorrect() {
        boolean checkResult = true;
        for (BaseCheck workGroup : workGroups) {
            boolean temp = workGroup.checkCorrect();
            if (!temp) {
                System.out.println(workGroup.getClass().getSimpleName() + "验证失败");
            }
            checkResult = checkResult & temp;
        }
        return checkResult;
    }
}
