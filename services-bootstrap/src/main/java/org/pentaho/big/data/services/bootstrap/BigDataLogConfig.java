/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/

package org.pentaho.big.data.services.bootstrap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.RollingFileAppender;
import org.apache.logging.log4j.core.appender.rolling.DefaultRolloverStrategy;
import org.apache.logging.log4j.core.appender.rolling.TimeBasedTriggeringPolicy;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.apache.logging.log4j.Level;

import java.util.Arrays;
import java.util.List;

/**
 * Configuration class for Big Data plugin logging.
 * 
 * The Big Data plugin attempts to use existing Pentaho/Kettle Log4j2 configuration
 * if Big Data loggers are already defined. If not, it programmatically adds loggers
 * and appenders to write logs to pdi.log and console.
 * 
 * This approach allows the plugin to work independently without requiring changes to
 * the main Kettle log4j2.xml configuration.
 */
public class BigDataLogConfig {

  private static boolean initialized = false;
  private static final Logger logger = LogManager.getLogger(BigDataLogConfig.class);

  // Big Data logger names that should write to application logs
  private static final List<LoggerDefinition> BIG_DATA_LOGGERS = Arrays.asList(
    new LoggerDefinition("com.pentaho.big.data", Level.INFO),
    new LoggerDefinition("org.pentaho.big.data", Level.INFO),
    new LoggerDefinition("org.apache.hadoop", Level.INFO),
    new LoggerDefinition("org.apache.hadoop.io.retry", Level.WARN),
    new LoggerDefinition("org.apache.hbase", Level.INFO),
    new LoggerDefinition("org.apache.hive", Level.INFO),
    new LoggerDefinition("org.apache.sqoop", Level.INFO),
    new LoggerDefinition("org.apache.kafka", Level.WARN),
    new LoggerDefinition("org.apache.spark", Level.WARN),
    new LoggerDefinition("com.pentaho.yarn", Level.INFO)
  );

  // Kettle/Spoon environment appender names
  private static final String PDI_APPENDER_NAME = "pdi-execution-appender";
  private static final String PENTAHO_CONSOLE_APPENDER_NAME = "PENTAHOCONSOLE";
  
  // Pentaho Server/Tomcat environment appender names
  private static final String PENTAHO_APPENDER_NAME = "PENTAHO";
  private static final String PENTAHO_SERVER_CONSOLE_APPENDER = "PENTAHOCONSOLE";
  
  private static final String BIG_DATA_APPENDER_NAME = "big-data-execution-appender";
  
  private static RuntimeEnvironment detectedEnvironment = null;

  /**
   * Initializes the Big Data logging - checks if loggers are already configured,
   * and if not, programmatically adds them to the running Log4j2 context.
   * This method is idempotent and can be safely called multiple times.
   */
  public static synchronized void initializeBigDataLogging() {
    if (initialized) {
      logger.debug("Big Data logging already initialized");
      return;
    }

    try {
      LoggerContext context = (LoggerContext) LogManager.getContext(false);
      Configuration config = context.getConfiguration();

      // Detect the runtime environment
      detectedEnvironment = detectEnvironment(config);
      logger.info("Detected runtime environment: {}", detectedEnvironment);

      // Check if Big Data loggers are already configured in the main log4j2.xml
      boolean loggersAlreadyConfigured = checkIfLoggersConfigured(config);

      if (loggersAlreadyConfigured) {
        logger.info("Big Data loggers already configured in main log4j2.xml - using existing configuration");
      } else {
        logger.info("Big Data loggers not found in main configuration - adding programmatically");
        configureBigDataLoggers(context, config);
      }

      initialized = true;

    } catch (Exception e) {
      logger.error("Failed to initialize Big Data logging", e);
      initialized = true;
    }
  }

  /**
   * Check if Big Data loggers are already configured in the Log4j2 configuration.
   * A logger is considered "configured" if it has explicit appender references.
   *
   * @param config the Log4j2 configuration
   * @return true if at least one Big Data logger has appender references
   */
  private static boolean checkIfLoggersConfigured(Configuration config) {
    for (LoggerDefinition loggerDef : BIG_DATA_LOGGERS) {
      LoggerConfig loggerConfig = config.getLoggerConfig(loggerDef.name);
      
      // Check if this specific logger (not parent) has appenders
      if (loggerConfig != null && loggerConfig.getName().equals(loggerDef.name)) {
        if (!loggerConfig.getAppenders().isEmpty()) {
          logger.debug("Found configured Big Data logger: {}", loggerDef.name);
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Detect the runtime environment (Kettle/Spoon or Pentaho Server/Tomcat).
   *
   * @param config the Log4j2 configuration
   * @return the detected environment
   */
  private static RuntimeEnvironment detectEnvironment(Configuration config) {
    // Check for Pentaho Server appender (PENTAHO)
    if (config.getAppender(PENTAHO_APPENDER_NAME) != null) {
      return RuntimeEnvironment.PENTAHO_SERVER;
    }
    
    // Check for Kettle appender (pdi-execution-appender)
    if (config.getAppender(PDI_APPENDER_NAME) != null) {
      return RuntimeEnvironment.KETTLE;
    }
    
    // Check system properties for additional hints
    String catalinaHome = System.getProperty("catalina.home");
    String catalinaBase = System.getProperty("catalina.base");
    if (catalinaHome != null || catalinaBase != null) {
      return RuntimeEnvironment.PENTAHO_SERVER;
    }
    
    // Default to Kettle if can't determine
    return RuntimeEnvironment.KETTLE;
  }

  /**
   * Programmatically configure Big Data loggers to write to application logs and console.
   *
   * @param context the LoggerContext
   * @param config  the Log4j2 configuration
   */
  private static void configureBigDataLoggers(LoggerContext context, Configuration config) {
    Appender fileAppender = null;
    Appender consoleAppender = null;

    // Get or create the appropriate file appender based on environment
    if (detectedEnvironment == RuntimeEnvironment.PENTAHO_SERVER) {
      // Pentaho Server environment
      fileAppender = config.getAppender(PENTAHO_APPENDER_NAME);
      consoleAppender = config.getAppender(PENTAHO_SERVER_CONSOLE_APPENDER);
      
      if (fileAppender == null) {
        logger.info("Creating PENTAHO appender for Big Data logs in Pentaho Server");
        fileAppender = createPentahoServerAppender(config);
        config.addAppender(fileAppender);
        fileAppender.start();
      } else {
        logger.debug("Using existing PENTAHO appender");
      }
    } else {
      // Kettle/Spoon environment
      fileAppender = config.getAppender(PDI_APPENDER_NAME);
      consoleAppender = config.getAppender(PENTAHO_CONSOLE_APPENDER_NAME);
      
      if (fileAppender == null) {
        logger.info("Creating pdi-execution-appender for Big Data logs in Kettle");
        fileAppender = createKettleAppender(config);
        config.addAppender(fileAppender);
        fileAppender.start();
      } else {
        logger.debug("Using existing pdi-execution-appender");
      }
    }

    // Add loggers for all Big Data components
    for (LoggerDefinition loggerDef : BIG_DATA_LOGGERS) {
      LoggerConfig loggerConfig = config.getLoggerConfig(loggerDef.name);
      
      // Only create new logger if it doesn't exist or is the root logger
      if (loggerConfig == null || !loggerConfig.getName().equals(loggerDef.name)) {
        logger.debug("Creating logger for: {}", loggerDef.name);
        
        LoggerConfig newLoggerConfig = LoggerConfig.createLogger(
          false, // additivity
          loggerDef.level,
          loggerDef.name,
          "true",
          new org.apache.logging.log4j.core.config.AppenderRef[0],
          null,
          config,
          null
        );
        
        // Add appenders
        newLoggerConfig.addAppender(fileAppender, loggerDef.level, null);
        if (consoleAppender != null) {
          newLoggerConfig.addAppender(consoleAppender, loggerDef.level, null);
        }
        
        config.addLogger(loggerDef.name, newLoggerConfig);
      } else {
        // Logger exists, just add our appenders if not already present
        logger.debug("Updating existing logger: {}", loggerDef.name);
        String existingAppenderName = detectedEnvironment == RuntimeEnvironment.PENTAHO_SERVER 
          ? PENTAHO_APPENDER_NAME : PDI_APPENDER_NAME;
        
        if (!loggerConfig.getAppenders().containsKey(existingAppenderName) && 
            !loggerConfig.getAppenders().containsKey(BIG_DATA_APPENDER_NAME)) {
          loggerConfig.addAppender(fileAppender, loggerDef.level, null);
          if (consoleAppender != null) {
            loggerConfig.addAppender(consoleAppender, loggerDef.level, null);
          }
        }
      }
    }

    // Update the configuration
    context.updateLoggers();
    
    String logDestination = detectedEnvironment == RuntimeEnvironment.PENTAHO_SERVER 
      ? "pentaho.log" : "pdi.log";
    logger.info("Big Data loggers configured - logs will appear in {} and console", logDestination);
  }

  /**
   * Create a RollingFileAppender for Kettle/PDI logs.
   *
   * @param config the Log4j2 configuration
   * @return the created Appender
   */
  private static Appender createKettleAppender(Configuration config) {
    PatternLayout layout = PatternLayout.newBuilder()
      .withPattern("%d{yyyy-MM-dd HH:mm:ss.SSS} %-5p <%t> [%c{1}] %m%n")
      .withConfiguration(config)
      .build();

    TimeBasedTriggeringPolicy policy = TimeBasedTriggeringPolicy.newBuilder()
      .build();

    DefaultRolloverStrategy strategy = DefaultRolloverStrategy.newBuilder()
      .withConfig(config)
      .build();

    return RollingFileAppender.newBuilder()
      .setName(BIG_DATA_APPENDER_NAME)
      .withFileName("logs/pdi.log")
      .withFilePattern("logs/pdi.%d{yyyy-MM-dd}.log")
      .setLayout(layout)
      .withPolicy(policy)
      .withStrategy(strategy)
      .setConfiguration(config)
      .build();
  }

  /**
   * Create a RollingFileAppender for Pentaho Server logs.
   *
   * @param config the Log4j2 configuration
   * @return the created Appender
   */
  private static Appender createPentahoServerAppender(Configuration config) {
    PatternLayout layout = PatternLayout.newBuilder()
      .withPattern("%d{yyyy-MM-dd HH:mm:ss.SSS} %-5p [%c{1}] %m%n")
      .withConfiguration(config)
      .build();

    TimeBasedTriggeringPolicy policy = TimeBasedTriggeringPolicy.newBuilder()
      .build();

    DefaultRolloverStrategy strategy = DefaultRolloverStrategy.newBuilder()
      .withConfig(config)
      .build();

    return RollingFileAppender.newBuilder()
      .setName(BIG_DATA_APPENDER_NAME)
      .withFileName("logs/pentaho.log")
      .withFilePattern("logs/pentaho.%d{yyyy-MM-dd}.log")
      .setLayout(layout)
      .withPolicy(policy)
      .withStrategy(strategy)
      .setConfiguration(config)
      .build();
  }

  /**
   * Gets a logger for the Big Data plugin with the configured appender.
   *
   * @param clazz the class requesting the logger
   * @return configured Logger instance
   */
  public static Logger getBigDataLogger(Class<?> clazz) {
    // Ensure initialization before returning logger
    if (!initialized) {
      initializeBigDataLogging();
    }
    return LogManager.getLogger(clazz);
  }

  /**
   * Gets a logger for the Big Data plugin with the configured appender.
   *
   * @param name the name for the logger
   * @return configured Logger instance
   */
  public static Logger getBigDataLogger(String name) {
    // Ensure initialization before returning logger
    if (!initialized) {
      initializeBigDataLogging();
    }
    return LogManager.getLogger(name);
  }

  /**
   * Checks if the Big Data logging has been initialized.
   *
   * @return true if initialized, false otherwise
   */
  public static boolean isInitialized() {
    return initialized;
  }

  /**
   * Resets the initialization flag (mainly for testing purposes).
   */
  protected static void resetInitialization() {
    initialized = false;
  }

  /**
   * Enum to represent the runtime environment.
   */
  private enum RuntimeEnvironment {
    KETTLE,          // Spoon/Kettle environment
    PENTAHO_SERVER   // Pentaho Server/Tomcat environment
  }

  /**
   * Internal class to define logger configurations.
   */
  private static class LoggerDefinition {
    final String name;
    final Level level;

    LoggerDefinition(String name, Level level) {
      this.name = name;
      this.level = level;
    }
  }
}
