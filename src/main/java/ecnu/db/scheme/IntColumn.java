package ecnu.db.scheme;


import ecnu.db.utils.ZipDistributionList;

import java.util.ArrayList;

/**
 * @author wangqingshuai
 * int的tuple相关类
 */
public class IntColumn extends AbstractColumn {

    private ZipDistributionList zipDistributionList;

    IntColumn(int range) {
        super(range, ColumnType.INT);
    }

    IntColumn(ArrayList<Integer> foreignKeys) {
        super(-1, ColumnType.INT);
        zipDistributionList = new ZipDistributionList(foreignKeys, false);
    }

    @Override
    public String getTableSQL() {
        return "INT";
    }

    @Override
    public Object getValue() {
        if (zipDistributionList != null) {
            return zipDistributionList.getValue();
        }
        return R.nextInt(range);
    }
}
