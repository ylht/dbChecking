package ecnu.db.check.group;

import ecnu.db.check.BaseCheckCorrectness;
import ecnu.db.check.WorkNode;
import ecnu.db.scheme.DecimalColumn;
import ecnu.db.transaction.Function;
import ecnu.db.utils.MysqlConnector;

import java.sql.SQLException;
import java.util.ArrayList;

public class FunctionCheckCorrectness extends BaseCheckCorrectness {
    private int k;
    private boolean add;
    private ArrayList<WorkNode> xNodes;
    private ArrayList<WorkNode> yNodes;
    private final static byte SELECT_TYPE = 0b1000;
    private final static byte SELECT_FOR_UPDATE = 0b1000;

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
        for (WorkNode node : xNodes) {
            node.setEndSum(mysqlConnector.sumColumn(node.getTableIndex(), node.getColumnIndex()));
        }
        for (WorkNode node : yNodes) {
            node.setEndSum(mysqlConnector.sumColumn(node.getTableIndex(), node.getColumnIndex()));
        }
    }

    @Override
    public boolean checkCorrect() {
        Double xBeginSum = 0d;
        Double xEndSum = 0d;

        for (WorkNode xNode : xNodes) {
            xBeginSum += xNode.getBeginSum();
            xEndSum += xNode.getEndSum();
        }

        Double yBeginSum=0d;
        Double yEndSum=0d;

        for (WorkNode yNode : yNodes) {
            yBeginSum+=yNode.getBeginSum();
            yEndSum+=yNode.getEndSum();
        }
        double ydiff=yEndSum-yBeginSum;
        double xdiff=xEndSum-xBeginSum;
        return DecimalColumn.getDf().format(ydiff).equals(DecimalColumn.getDf().format(k*xdiff));
    }
}
