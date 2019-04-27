package ecnu.db.config;

public class SystemConfig extends ReadConfig {
    private static SystemConfig config;

    private SystemConfig(String fileName) {
        super(fileName);
    }

    public synchronized static SystemConfig getConfig() {
        if (config == null) {
            config = new SystemConfig("config/SystemConfig.xml");
        }
        return config;
    }

    public static void setConfig(String configFileName) {
        config = new SystemConfig(configFileName);
    }

    public double getZipf() {
        return Double.valueOf(document.valueOf("generator/transaction/zipf"));
    }


    public int getRunThreadNumOnCore() {
        return Integer.valueOf(document.valueOf("generator/thread/runThreadNumOnCore"));
    }

    public int getThreadNum() {
        return getRunThreadNumOnCore() * Runtime.getRuntime().availableProcessors();
    }

    public int getRunCount() {
        return Integer.valueOf(document.valueOf("generator/thread/runCount"));
    }


}
