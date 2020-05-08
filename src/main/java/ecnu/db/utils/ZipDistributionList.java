package ecnu.db.utils;

import ecnu.db.config.SystemConfig;
import ecnu.db.config.TableConfig;
import org.apache.commons.math3.distribution.ZipfDistribution;

import java.util.ArrayList;
import java.util.Collections;

public class ZipDistributionList {
    private final ArrayList<Integer> list;
    private final ZipfDistribution zf;

    public ZipDistributionList(ArrayList<Integer> list, boolean forTransaction) {
        this.list = new ArrayList<>(list);
        Collections.shuffle(list);
        if (forTransaction) {
            zf = new ZipfDistribution(list.size(), SystemConfig.getConfig().getZipf());
        } else {
            zf = new ZipfDistribution(list.size(), TableConfig.getConfig().getZipf());
        }

    }

    public int getValue() {
        return list.get(zf.sample() - 1);
    }
}
