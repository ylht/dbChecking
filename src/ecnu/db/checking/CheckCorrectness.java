package ecnu.db.checking;

import ecnu.db.utils.LoadConfig;

import java.util.ArrayList;

public class CheckCorrectness {
    private ArrayList<WorkGroup> workGroups;

    CheckCorrectness(){
        workGroups= LoadConfig.getConfig().getWorkNode();
    }

    public void printWorkGroup() {
        for(WorkGroup workGroup:workGroups){
            System.out.println(workGroup);
        }
    }

    public static void main(String[] args){
        CheckCorrectness checkCorrectness=new CheckCorrectness();
        checkCorrectness.printWorkGroup();
    }
}
