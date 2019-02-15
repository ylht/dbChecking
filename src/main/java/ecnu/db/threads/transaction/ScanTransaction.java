package ecnu.db.threads.transaction;

import ecnu.db.scheme.Table;
import ecnu.db.utils.MysqlConnector;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.CountDownLatch;

public class ScanTransaction implements Runnable {
    private Table table;
    private int tupleIndex;
    private int runCount;
    private CountDownLatch count;
    private MysqlConnector mysqlConnector=new MysqlConnector();
    public ScanTransaction(Table table, int tupleIndex, int runCount, CountDownLatch count){
        this.table=table;
        this.tupleIndex=tupleIndex;
        this.runCount=runCount;
        this.count=count;
    }

    @Override
    public void run() {
        PreparedStatement scanStatement=mysqlConnector.getScanStatement(table.getTableIndex(),tupleIndex);
        Connection conn=mysqlConnector.getConn();
        for(int i=0;i<runCount;i++){
            double min=table.getRandomValue(tupleIndex);
            double max=table.getRandomValue(tupleIndex);
            if(max<min){
                max=max+min;
                min=max-min;
                max=max-min;
            }
            assert max>=min;
            try {
                scanStatement.setDouble(1,min);
                scanStatement.setDouble(2,max);
                ResultSet rs = scanStatement.executeQuery();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
