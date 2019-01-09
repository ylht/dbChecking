package ecnu.db.scheme;

import ecnu.db.scheme.AbstractTuple;
import ecnu.db.scheme.DoubleTuple;
import ecnu.db.scheme.IntTuple;
import ecnu.db.utils.LoadConfig;
import org.dom4j.Node;

import java.util.ArrayList;
import java.util.List;

public class Table {
    private ArrayList<AbstractTuple> tuples=new ArrayList<>();
    private int tableIndex;


    public Table(int tableIndex) {
        this.tableIndex = tableIndex;
        List<Node> nodes=LoadConfig.getConfig().getTableIntTupleInfo(tableIndex);
        int i=0;
        for(Node node :nodes){
            switch (node.valueOf("type")){
                case "int":
                    tuples.add(new IntTuple(i,Integer.parseInt(node.valueOf("min")),
                            Integer.parseInt(node.valueOf("range"))));
                    break;
                case "double":
                    tuples.add(new DoubleTuple(i,Integer.parseInt(node.valueOf("min")),
                            Integer.parseInt(node.valueOf("range"))));
                    break;
                default:
                    System.out.println("配置文件错误");
                    System.exit(-1);
            }
        }
    }

    public String getSQL(){
        StringBuilder sql= new StringBuilder("CREATE TABLE t" + tableIndex + "(");
        for(AbstractTuple tuple:tuples){
            sql.append(tuple.getTableSQL());
        }
        //将SQL句尾的逗号替换为括号
        return sql.substring(0,sql.length()-1)+")";
    }
}
