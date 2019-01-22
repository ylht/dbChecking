package ecnu.db.threads;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ThreadPool {
    //初始化线程池
    private static final int CORE_NUM = Runtime.getRuntime().availableProcessors();
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
            new MyThreadFactory());

    public static ThreadPoolExecutor getThreadPoolExecutor() {
        return THREAD_POOL_EXECUTOR;
    }
}
