package ecnu.db.schema;

public class FloatColumn extends AbstractColumn {
    FloatColumn(int range) {
        super(range, ColumnType.FLOAT);
    }

    @Override
    public String getTableSQL() {
        return "FLOAT";
    }

    @Override
    Object getValue() {
        return Math.random() * range;
    }
}
