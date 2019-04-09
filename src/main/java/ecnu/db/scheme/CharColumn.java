package ecnu.db.scheme;

public class CharColumn extends AbstractColumn {

    private boolean isVarchar;

    public CharColumn(int min, int range, boolean isVarchar) {
        super(min, range);
        this.isVarchar = isVarchar;
    }

    @Override
    public String getTableSQL() {
        if (isVarchar) {
            return "VARCHAR(" + (min + range) + ")";
        } else {
            return "CHAR(" + (min + range) + ")";
        }
    }


    @Override
    public Object getValue(boolean processingTableData) {
        StringBuilder value = new StringBuilder();
        int length = min + R.nextInt(range);
        for (int i = 0; i < length; i++) {
            //从第33号字符到第126号字符，包含所有的非空字符
            value.append((char) (33 + R.nextInt(94)));
        }
        return value;
    }
}
