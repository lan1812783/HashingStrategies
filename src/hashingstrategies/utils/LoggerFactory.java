package hashingstrategies.utils;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

public class LoggerFactory {
	
	public static Logger getLogger(Class clazz) {
		ConsoleAppender ca = new ConsoleAppender(new PatternLayout("%p\t%m%n"));
		ca.setName("ConsoleLogger");
		ca.setTarget("System.out");
		
		Logger logger = Logger.getLogger(clazz);
		logger.addAppender(ca);
		logger.setLevel(Level.INFO);
		
		return logger;
	}
}
