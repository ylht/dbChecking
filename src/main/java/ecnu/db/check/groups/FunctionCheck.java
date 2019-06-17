package ecnu.db.check.groups;

import ecnu.db.check.BaseCheck;
import ecnu.db.check.CheckNode;
import ecnu.db.transaction.Function;
import ecnu.db.utils.MysqlConnector;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.ArrayList;

public class FunctionCheck extends BaseCheck {
    private int k;
    private boolean add;
    private ArrayList<CheckNode> xNodes;
    private ArrayList<CheckNode> yNodes;

    public FunctionCheck() {
        super("FunctionConfig.xml");
    }


    @Override
    public void makeTransaction() {
        xNodes = new ArrayList<>();
        yNodes = new ArrayList<>();
        boolean state = false;
        for (CheckNode checkNode : checkNodes) {
            if (state) {
                xNodes.add(checkNode);
            } else {
                yNodes.add(checkNode);
            }
            state = !state;
        }
        try {
            k = config.getK();
            if (k < 1) {
                throw new Exception("非法的k值");
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("读取K值错误，设置为默认值为1");
            k = 1;
        }
        add = config.addOrNot();
        transaction = new Function(columnType, xNodes, yNodes, checkConfigWorkOrNot("select"),
                checkConfigWorkOrNot("selectWithForUpdate"), add, k);
    }

    @Override
    public void recordBeginStatus(MysqlConnector mysqlConnector) throws SQLException {
        for (CheckNode node : checkNodes) {
            node.setBeginSum(mysqlConnector.sumColumn(node.getTableIndex(), node.getColumnIndex()));
        }
    }

    @Override
    public void recordEndStatus(MysqlConnector mysqlConnector) throws SQLException {
        for (CheckNode node : checkNodes) {
            node.setEndSum(mysqlConnector.sumColumn(node.getTableIndex(), node.getColumnIndex()));
        }
    }

    @Override
    public boolean checkCorrect() {
        BigDecimal xBeginSum = new BigDecimal(0);
        BigDecimal xEndSum = new BigDecimal(0);

        for (CheckNode xNode : xNodes) {
            xBeginSum = xBeginSum.add(BigDecimal.valueOf(xNode.getBeginSum()));
            xEndSum = xEndSum.add(BigDecimal.valueOf(xNode.getEndSum()));
        }

        BigDecimal yBeginSum = new BigDecimal(0);
        BigDecimal yEndSum = new BigDecimal(0);

        for (CheckNode yNode : yNodes) {
            yBeginSum = yBeginSum.add(BigDecimal.valueOf(yNode.getBeginSum()));
            yEndSum = yEndSum.add(BigDecimal.valueOf(yNode.getEndSum()));
        }
        BigDecimal xSub = xEndSum.subtract(xBeginSum);
        BigDecimal ySub = yEndSum.subtract(yBeginSum);
        return (ySub).equals((xSub).multiply(new BigDecimal(k)));
    }
}
