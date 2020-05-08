package ecnu.db;

import ecnu.db.check.BaseCheck;
import org.apache.logging.log4j.LogManager;

/**
 * @author wangqingshuai
 * 项目执行的主函数
 */
public class Main {

    public static void main(String[] args) {
        //初始化logger
        LogManager.getLogger();
        //项目执行的核心类
        DbChecking dbChecking = new DbChecking(BaseCheck.CheckKind.RepeatableRead);
        //初始化scheme
        dbChecking.createScheme();
        //载入数据
        dbChecking.loadData();
        dbChecking.check();
    }
}
