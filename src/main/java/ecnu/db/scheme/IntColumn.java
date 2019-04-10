package ecnu.db.scheme;


import org.apache.commons.math3.distribution.ZipfDistribution;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

/**
 * @author wangqingshuai
 * int的tuple相关类
 */
public class IntColumn extends AbstractColumn {
    private ZipfDistribution zipfDistribution;
    private ArrayList<Integer> foreignKeys;
    IntColumn(int range) {
        super(range);
    }

    IntColumn(ArrayList<Integer> foreignKeys) {
        super(-1);
        this.foreignKeys=new ArrayList<>(foreignKeys);
        Collections.shuffle(this.foreignKeys);
        zipfDistribution=new ZipfDistribution(foreignKeys.size(),1);
    }

    @Override
    public String getTableSQL() {
        return "INT";
    }

    @Override
    public Object getValue(boolean processingTableData) {
        if (zipfDistribution != null) {
            return foreignKeys.get(zipfDistribution.sample()-1);
        }
        if (processingTableData) {
            return R.nextInt(range);
        } else {
            return R.nextInt(range / RANGE_RANDOM_COUNT);
        }
    }
}
