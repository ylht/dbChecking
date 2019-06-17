package ecnu.db.check;

import ecnu.db.config.TransactionConfig;
import ecnu.db.scheme.AbstractColumn;
import ecnu.db.transaction.BaseTransaction;
import ecnu.db.utils.MysqlConnector;

import java.sql.SQLException;
import java.util.ArrayList;

/**
 * @author wangqingshuai
 * 工作组类，用于区别在同一组事务中存在的列
 * <p>
 * 事务的操作主要有三种
 * 1. 转账事务 在不同列之间互相转移数据
 * 2. y=kx+b事务，在两个列之间维持该关系式
 * 3. 订单事务，在单列上的tuple上累加，并写入日志，判定日志与服务器的数据是否一致。
 */
public abstract class BaseCheck {
    protected ArrayList<CheckNode> checkNodes = new ArrayList<>();
    protected AbstractColumn.ColumnType columnType;
    protected BaseTransaction transaction;
    private CheckKind checkKind;
    protected TransactionConfig config;

    public BaseCheck(String configName) {
        config = TransactionConfig.getConfig(configName);
        try {
            columnType = config.getColumnTypeForTransacion();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 是否需要在此隔离级别下执行
     *
     * @param checkKind 需要测试的隔离级别
     * @return true为需要判定，false为不需要判定
     */
    public boolean checkOrNot(CheckKind checkKind) throws Exception {
        Byte testType = config.getTransactionCheckType();
        if (testType == null) {
            throw new Exception("需要为" + this.getClass().getSimpleName() + "测试样例指定测试级别");
        }
        this.checkKind = checkKind;
        return workOnTheCheckKind(testType, checkKind);
    }

    /**
     * @return 获取该工作组的事务
     */
    public BaseTransaction getTransaction() throws Exception {
        if (transaction != null) {
            return transaction;
        }
        throw new Exception("没有初始化事务");
    }

    public void addCheckNode(CheckNode node) {
        checkNodes.add(node);
    }


    /**
     * 返回该工作组本次需要操作的数据列的类型
     *
     * @return 数据列类型。
     */
    public AbstractColumn.ColumnType columnType() {
        return columnType;
    }

    /**
     * 根据工作组需要的列添加workNode
     *
     * @return 该工作组需要操作几个列
     */
    public int getColumnCount() {
        int columnCount = 0;
        try{
            columnCount=config.getColumnNumForTransaction();
        }catch (Exception e){
            System.out.println("读取columnCount失败");
        }
        return columnCount;
    }

    /**
     * 本地根据schema信息构建事务
     */
    public abstract void makeTransaction();


    /**
     * 记录工作组开始执行前的数据库数据
     *
     * @param mysqlConnector 加载数据库驱动
     * @throws SQLException 抛出异常
     */
    public abstract void recordBeginStatus(MysqlConnector mysqlConnector) throws SQLException;

    /**
     * 记录工作组执行之后的数据库数据
     *
     * @param mysqlConnector 加载数据库驱动
     * @throws SQLException 抛出异常
     */
    public abstract void recordEndStatus(MysqlConnector mysqlConnector) throws SQLException;

    /**
     * 该工作组是否维持了一致性
     *
     * @return 满足返回true否则返回false
     */
    public abstract boolean checkCorrect();

    protected Boolean checkConfigWorkOrNot(String typeName) {
        byte configType = 0;
        try {
            configType = config.getConfigCheckType(typeName);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return workOnTheCheckKind(configType, checkKind);
    }

    public boolean getWholeTable() {
        return config.getWholeTable();
    }

    public boolean getColumnFromSameTable() {
        return config.getColumnFromSameTable();
    }

    public boolean columnNumNotEnough() {
        return checkNodes.size() < config.getMinColumnNum();
    }


    private Boolean workOnTheCheckKind(Byte workType, CheckKind checkKind) {
        switch (checkKind) {
            case ReadUncommitted:
                return (workType & 0b1000) == 0b1000;
            case ReadCommitted:
                return (workType & 0b0100) == 0b0100;
            case RepeatableRead:
                return (workType & 0b0010) == 0b0010;
            case Serializable:
                return (workType & 0b0001) == 0b0001;
            default:
                System.out.println("不支持检测此并发控制协议");
                return false;
        }
    }

    @Override
    public String toString() {
        StringBuilder workNodeInfo = new StringBuilder();
        for (CheckNode checkNode : checkNodes) {
            workNodeInfo.append(checkNode.toString());
        }
        return this.getClass().getSimpleName() + " " + workNodeInfo;
    }

    public enum CheckKind {
        /**
         * 需要测试的隔离级别，分别为，读未提交，读已提交，可重复读，冲突可串行化
         */
        ReadUncommitted, ReadCommitted, RepeatableRead, Serializable
    }
}
