package ecnu.db.scheme;

import java.util.Random;

public class VarcharColumn extends AbstractColumn {

    VarcharColumn(int range) {
        super(range, ColumnType.VARCHAR);
    }

    @Override
    public String getTableSQL() {
        return "VARCHAR(" + range + ")";
    }

    @Override
    public Object getValue() {
        StringBuilder value = new StringBuilder();
        int length = new Random().nextInt(range) + 1;
        for (int i = 0; i < length; i++) {
            //从第33号字符到第126号字符，包含所有的非空字符
            value.append((char) (97 + new Random().nextInt(25)));
        }
        return value;
    }
}
