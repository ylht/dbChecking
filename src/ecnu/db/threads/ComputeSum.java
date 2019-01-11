package ecnu.db.threads;

import ecnu.db.checking.WorkGroup;
import ecnu.db.checking.WorkNode;
import ecnu.db.utils.MysqlConnector;

import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;

public class ComputeSum implements Runnable {
    private WorkGroup workGroup;
    private CountDownLatch count;
    private Boolean isBegin;
    public ComputeSum(WorkGroup workGroup, CountDownLatch count, boolean isBegin){
        this.workGroup=workGroup;
        this.count=count;
        this.isBegin=isBegin;
    }

    @Override
    public void run() {
        MysqlConnector mysqlConnector=new MysqlConnector();
        ArrayList<WorkNode> allNodes=workGroup.getAllNode();
        if(isBegin){
            for(WorkNode node:allNodes){
                node.setBeginSum(mysqlConnector.sumColumn(node.getTableIndex(),node.getTupleIndex()));
            }
        }else {
            for(WorkNode node:allNodes){
                node.setEndSum(mysqlConnector.sumColumn(node.getTableIndex(),node.getTupleIndex()));
            }
        }
        count.countDown();
        mysqlConnector.close();
    }
}
