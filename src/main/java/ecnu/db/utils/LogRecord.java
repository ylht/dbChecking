package ecnu.db.utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class LogRecord {
    public static final Logger logger= LogManager.getLogger();

    public static void main(String[] args){
        logger.trace("1 2 43 31");
        logger.trace("1 2 43 312");
    }
}
