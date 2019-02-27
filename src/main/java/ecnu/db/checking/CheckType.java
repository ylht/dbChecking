package ecnu.db.checking;

public class CheckType {
    private CheckKind checkKind;
    private boolean updateWithSelect;
    private boolean forUpdate;
    private boolean scan;
    private boolean scanCheckReadUncommited;

    public CheckType(CheckKind checkKind) {
        this.checkKind = checkKind;
        switch (checkKind) {
            case ReadUncommited:
                updateWithSelect = false;
                scan = false;
                break;
            case ReadCommited:
                updateWithSelect = false;
                scan = true;
                scanCheckReadUncommited = true;
                break;
            case ReaptableRead:
                updateWithSelect = true;
                forUpdate = false;
                scan = false;
                break;
            case Serializable:
                updateWithSelect = true;
                forUpdate = false;
                scan = true;
                scanCheckReadUncommited = false;
                break;
            default:
                updateWithSelect = true;
                forUpdate = false;
                scan = true;
                scanCheckReadUncommited = false;
        }
    }

    public CheckKind getCheckKind() {
        return checkKind;
    }

    public boolean isUpdateWithSelect() {
        return updateWithSelect;
    }

    public boolean isForUpdate() {
        return forUpdate;
    }

    public boolean isScan() {
        return scan;
    }

    public boolean isScanCheckReadUncommited() {
        return scanCheckReadUncommited;
    }


    public enum CheckKind {
        /**
         * 需要测试的隔离级别，分别为，读未提交，读已提交，可重复读，冲突可串行化
         */
        ReadUncommited, ReadCommited, ReaptableRead, Serializable
    }


}
