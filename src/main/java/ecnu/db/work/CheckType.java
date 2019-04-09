package ecnu.db.work;

public class CheckType {
    private CheckKind checkKind;
    private boolean updateWithSelect = false;
    private boolean forUpdate;
    private boolean checkRepeatableRead = false;
    private boolean checkNoCommitted = false;
    private boolean checkWriteSkew = false;
    private boolean checkPhantomRead = false;

    public CheckType(CheckKind checkKind) {
        this.checkKind = checkKind;
        switch (checkKind) {
            case ReadUncommitted:
                break;
            case ReadCommitted:
                checkNoCommitted = true;
                break;
            case RepeatableRead:
                checkNoCommitted = true;
                checkRepeatableRead = true;
                break;
            case Serializable:
                updateWithSelect = true;
                checkWriteSkew = true;
                checkPhantomRead = true;
                forUpdate = false;
                break;
            default:
                break;
        }
    }

    public boolean isCheckPhantomRead() {
        return checkPhantomRead;
    }

    public boolean isCheckRepeatableRead() {
        return checkRepeatableRead;
    }

    public boolean isCheckNoCommitted() {
        return checkNoCommitted;
    }

    public boolean isCheckWriteSkew() {
        return checkWriteSkew;
    }

    CheckKind getCheckKind() {
        return checkKind;
    }

    public boolean isUpdateWithSelect() {
        return updateWithSelect;
    }

    public boolean isForUpdate() {
        return forUpdate;
    }


    public enum CheckKind {
        /**
         * 需要测试的隔离级别，分别为，读未提交，读已提交，可重复读，冲突可串行化
         */
        ReadUncommitted, ReadCommitted, RepeatableRead, Serializable
    }


}
