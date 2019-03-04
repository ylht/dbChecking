package ecnu.db;

import ecnu.db.work.CheckType;
import ecnu.db.work.DbChecking;
import ecnu.db.threads.pool.DbCheckingThreadPool;
import ecnu.db.utils.LoadConfig;
import org.apache.logging.log4j.LogManager;

/**
 * @author wangqingshuai
 * 项目执行的主函数
 */
public class Main {

    public static void main(String[] args) {
        //初始化logger
        LogManager.getLogger();
        //载入配置文件
        LoadConfig.loadConfig("config/SingleTableCheckConfig.xml");
        //项目执行的核心类
        DbChecking dbChecking = new DbChecking(new CheckType(CheckType.CheckKind.Serializable));
        //初始化scheme
        dbChecking.createScheme();
        //载入数据
        dbChecking.loadData();
        dbChecking.check();
        DbCheckingThreadPool.closeThreadPool();
    }


}
