package ecnu.db.checking;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;

public class WorkGroup {

    private int workId;
    private ArrayList<WorkNode> in=new ArrayList<>();
    private ArrayList<WorkNode> out=new ArrayList<>();
    private ArrayList<WorkNode> inout=new ArrayList<>();

    public WorkGroup(int workId){
        this.workId=workId;
    }

    public void addInTuple(WorkNode in){
        this.in.add(in);
    }

    public void addOutTuple(WorkNode out){
        this.out.add(out);
    }

    public void addInoutTuple(WorkNode inout){
        this.inout.add(inout);
    }

    public int getWorkId(){
        return workId;
    }

    @Override
    public String toString(){
       return  "第"+workId+"工作组的数据为\n"+ "In:"+ Arrays.toString(in.toArray())+"\n"+
       "Out:"+Arrays.toString(out.toArray())+"\n"+ "Inout:"+Arrays.toString(inout.toArray());
    }
}
