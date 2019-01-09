package ecnu.db;

import ecnu.db.tuple.AbstractTuple;
import ecnu.db.tuple.IntTuple;
import ecnu.db.utils.LoadConfig;
import org.dom4j.Node;

import java.util.ArrayList;
import java.util.List;

public class Table {
    private ArrayList<AbstractTuple> tuples;
    private int tableIndex;


    public Table(int tableIndex) {
        this.tableIndex = tableIndex;
        List<Node> nodes=LoadConfig.getConfig().getTableIntTupleInfo(tableIndex);
        int i=0;
        for(Node node :nodes){
            switch (node.valueOf(''))
        }
    }

    public String getSQL(){
        String sql="CREATE TABLE t"+ tableIndex + "(";
        for(AbstractTuple tuple:tuples){
            sql+=tuple.getTableSQL();
        }
        return sql;
    }
}
