package ecnu.db.transaction;

import ecnu.db.checking.WorkGroup;
import ecnu.db.checking.WorkNode;
import ecnu.db.scheme.Table;
import ecnu.db.utils.MysqlConnector;
import org.apache.logging.log4j.LogManager;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author wangqingshuai
 * 处理fuction类型事务的线程
 */
public class FunctionTransaction extends BaseTransaction {

    private int k;
    private Table[] tables;
    private WorkNode inNode;
    private WorkNode outNode;
    private PreparedStatement addInStatement;
    private PreparedStatement addOutStatement;

    public FunctionTransaction(Table[] tables, WorkGroup workGroup, MysqlConnector mysqlConnector) {
        super(mysqlConnector);
        //确保工作组的类型正确
        assert workGroup.getWorkGroupType() == WorkGroup.WorkGroupType.function;
        mysqlConnector = new MysqlConnector();
        this.tables = tables;
        inNode = workGroup.getIn().get(0);
        outNode = workGroup.getOut().get(0);

        addInStatement = mysqlConnector.getRemittanceUpdate(true, inNode.getTableIndex(),
                inNode.getTupleIndex(), false);

        addOutStatement = mysqlConnector.getRemittanceUpdate(true, outNode.getTableIndex(),
                outNode.getTupleIndex(), false);

        this.k = workGroup.getK();
    }

    @Override
    public void execute() {
        Double subNum = tables[outNode.getTableIndex()].getTransactionValue(outNode.getTupleIndex());
        try {
            RemittanceTransaction.workForTransaction(true, tables, conn,
                    addOutStatement, outNode, subNum,
                    addInStatement, inNode, subNum * k);
        } catch (SQLException e) {
            LogManager.getLogger().error(e);
        }

    }
}
