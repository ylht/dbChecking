package ecnu.db.threads.pool;

import ecnu.db.config.SystemConfig;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author wangqingshuai
 * 全局线程池的基本配置信息
 */
public class DbCheckingThreadPool {
    private static final int CORE_NUM = SystemConfig.getConfig().getRunThreadNumOnCore()
            * Runtime.getRuntime().availableProcessors();
    private static final int MAX_POOL_SIZE = 2 * CORE_NUM;
    private static final int KEEP_ALIVE_TIME = 5000;
    /**
     * 线程池
     */
    private static final ThreadPoolExecutor THREAD_POOL_EXECUTOR = new ThreadPoolExecutor(CORE_NUM,
            MAX_POOL_SIZE,
            KEEP_ALIVE_TIME,
            TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(),
            new DbCheckingThreadFactory());

    public static ThreadPoolExecutor getThreadPoolExecutor() {
        return THREAD_POOL_EXECUTOR;
    }

    public static void closeThreadPool() {
        THREAD_POOL_EXECUTOR.shutdown();
    }
}
