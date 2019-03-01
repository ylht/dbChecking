package ecnu.db;

import ecnu.db.core.DbChecking;
import ecnu.db.core.CheckType;
import ecnu.db.scheme.Table;
import ecnu.db.threads.pool.DbCheckingThreadPool;
import ecnu.db.utils.LoadConfig;
import org.apache.logging.log4j.LogManager;

import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author wangqingshuai
 * 项目执行的主函数
 */
public class Main {

    public static void main(String[] args) {
        //初始化logger
        LogManager.getLogger();
        //载入配置文件
        LoadConfig.getConfig("config/SingleTableCheckConfig.xml");
        //项目执行的核心类
        DbChecking dbChecking=new DbChecking();
        //初始化scheme
        dbChecking.createScheme();
        //载入数据
        dbChecking.loadData();
        dbChecking.setCheckType(new CheckType(CheckType.CheckKind.Serializable));
        dbChecking.check();
        DbCheckingThreadPool.closeThreadPool();
    }



}
