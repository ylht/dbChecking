package ecnu.db.dataCreation;

import java.util.Random;

public class doubleCreation extends AbstractValueCreation {
    private double min;
    private double range;
    private static Random r=new Random();

    public doubleCreation(double min, double range) {
        this.min = min;
        this.range = range;
    }

    @Override
    public Object getValue() {
        return min+r.nextDouble()*range;
    }
}
