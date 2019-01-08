package ecnu.db.dataCreation;

import java.util.Random;

public class intCreation extends AbstractValueCreation {
    private int min;
    private int range;
    private static Random r=new Random();

    public intCreation(int min, int range) {
        this.min = min;
        this.range = range;
    }

    @Override
    public Object getValue() {
        return min+r.nextInt(range);
    }
}
