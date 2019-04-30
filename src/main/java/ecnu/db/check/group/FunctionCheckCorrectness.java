package ecnu.db.check.group;

import ecnu.db.check.BaseCheckCorrectness;
import ecnu.db.check.WorkNode;
import ecnu.db.scheme.DecimalColumn;
import ecnu.db.transaction.Function;
import ecnu.db.utils.MysqlConnector;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.ArrayList;

public class FunctionCheckCorrectness extends BaseCheckCorrectness {
    private int k;
    private boolean add;
    private ArrayList<WorkNode> xNodes;
    private ArrayList<WorkNode> yNodes;

    public FunctionCheckCorrectness() {
        super("FunctionConfig.xml");
    }


    @Override
    public void makeTransaction() {
        xNodes = new ArrayList<>();
        yNodes = new ArrayList<>();
        boolean state = false;
        for (WorkNode workNode : workNodes) {
            if (state) {
                xNodes.add(workNode);
            } else {
                yNodes.add(workNode);
            }
            state = !state;
        }
        try {
            k = config.getK();
            if(k<1){
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
        for (WorkNode node : workNodes) {
            node.setBeginSum(mysqlConnector.sumColumn(node.getTableIndex(), node.getColumnIndex()));
        }
    }

    @Override
    public void recordEndStatus(MysqlConnector mysqlConnector) throws SQLException {
        for (WorkNode node : workNodes) {
            node.setEndSum(mysqlConnector.sumColumn(node.getTableIndex(), node.getColumnIndex()));
        }
    }

    @Override
    public boolean checkCorrect() {
        BigDecimal xBeginSum = new BigDecimal(0);
        BigDecimal xEndSum = new BigDecimal(0);

        for (WorkNode xNode : xNodes) {
            xBeginSum=xBeginSum.add(BigDecimal.valueOf(xNode.getBeginSum()));
            xEndSum=xEndSum.add(BigDecimal.valueOf(xNode.getEndSum()));
        }

        BigDecimal yBeginSum=new BigDecimal(0);
        BigDecimal yEndSum=new BigDecimal(0);

        for (WorkNode yNode : yNodes) {
            yBeginSum=yBeginSum.add(BigDecimal.valueOf(yNode.getBeginSum()));
            yEndSum=yEndSum.add(BigDecimal.valueOf(yNode.getEndSum()));
        }
        BigDecimal xSub=xEndSum.subtract(xBeginSum);
        BigDecimal ySub=yEndSum.subtract(yBeginSum);
        return (ySub).equals((xSub).multiply(new BigDecimal(k)));
    }
}
