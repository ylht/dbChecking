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

    public String getDatabaseVersion() {
        return document.valueOf("generator/database");
    }

    public String getDatabaseurl() {
        return document.valueOf("generator/database/url");
    }

    public String getDatabaseName() {
        return document.valueOf("generator/database/name");
    }

    public String getDatabaseUser() {
        return document.valueOf("generator/database/user");
    }

    public String getDatabasePassword() {
        return document.valueOf("generator/database/password");
    }

    public double getZipf() {
        return Double.parseDouble(document.valueOf("generator/transaction/zipf"));
    }


    public int getRunThreadNumOnCore() {
        return Integer.parseInt(document.valueOf("generator/thread/runThreadNumOnCore"));
    }

    public int getThreadNum() {
        return getRunThreadNumOnCore() * Runtime.getRuntime().availableProcessors();
    }

    public int getRunCount() {
        return Integer.parseInt(document.valueOf("generator/thread/runCount"));
    }


}
