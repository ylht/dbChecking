package ecnu.db.utils;

import java.util.Random;

public class RandomTupleSize {
    private int totalTableNum;
    private int totalTupleNum;
    private Random r = new Random();
    public RandomTupleSize(int totalTableNum,int totalTupleNum){
        this.totalTableNum =totalTableNum;
        this.totalTupleNum=totalTupleNum;
    }

    public int getTupleSize(){
        if(totalTableNum ==1){
            return totalTupleNum;
        }else {
            int min=1;
            double max=(double) totalTupleNum/totalTableNum--*2;
            double size=r.nextDouble() * max;
            size=size<=min?min:size;
            totalTupleNum-=size;
            return (int)size;
        }
    }
}
