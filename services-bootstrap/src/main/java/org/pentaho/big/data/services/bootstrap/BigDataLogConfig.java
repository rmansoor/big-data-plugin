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

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

ream;


import java.util.ArrayList;
/**
 * Configuration class for Big Data plugin logging.
 * 
 * The Big Data plugin attempts to use existing Pentaho/Kettle Log4j2 configuration
 * if Big Data loggers are already defined. If not, it programmatically adds loggers
 * and appenders to write logs to pdi.log and console.
 * 
 * Logger definitions are loaded from bigdata-logging.properties file, and additional
 * loggers can be registered programmatically before initialization.
 * 
 * This approach allows the plugin to work independently without requiring changes to
 * the main Kettle log4j2.xml configuration.
 */
public class BigDataLogConfig {

    private static boolean initialized = false;
    private static final Logger logger = LogManager.getLogger(BigDataLogConfig.class);

    // Properties file containing logger definitions
    private static final String LOGGING_PROPERTIES_FILE = "bigdata-logging.properties";
    private static final String LOGGER_PREFIX = "logger.";

    // Registered loggers - allows dynamic registration before initialization
    private static final Map<String, Level> registeredLoggers = new ConcurrentHashMap<>();

    // Loaded logger definitions from properties file and registrations
    private static List<LoggerDefinition> bigDataLoggers = null;
    / Registered loggers - allows dynamic registration before initialization
    private static final Map<String, Level> registeredLoggers = new ConcurrentHashMap<>();

    // Loaded logger definitions from properties file and registrations
    private static List<LoggerDefinition> bigDataLoggers = null;

    // Kettle/Spoon environment appender names
    private static final String PDI_APPENDER_NAME = "pdi-execution-appender";
    private static final String PENTAHO_CONSOLE_APPENDER_NAME = "PENTAHOCONSOLE";

    // Pentaho Server/Tomcat environment appender names
    private static final String PENTAHO_APPENDER_NAME = "PENTAHO";
    private static RuntimeEnvironment detectedEnvironment = null;

    /**
     * Register a logger to be configured during initialization. This method
     * must be called before initializeBigDataLogging() is invoked.
     *
     * @param loggerName the fully qualified logger name (e.g.,
     * "org.apache.hbase")
     * @param level the log level for this logger
     * @return true if registered successfully, false if already initialized
     */
    public static synchronized boolean registerLogger(String loggerName, Level level) {
        if (initialized) {
            logger.warn("Cannot register logger '{}' - Big Data logging already initialized", loggerName);
            return false;
        }

        registeredLoggers.put(loggerName, level);
        logger.debug("Registered logger: {} with level: {}", loggerName, level);
        return true;
    }

    /**
     * Register a logger with INFO level to be configured during initialization.
     * This method must be called before initializeBigDataLogging() is invoked.
     *
     * @param loggerName the fully qualified logger name (e.g.,
     * "org.apache.hbase")
     * @return true if registered successfully, false if already initialized
     */
    public static synchronized boolean registerLogger(String loggerName) {
        return registerLogger(loggerName, Level.INFO);
    }

    /**
     * Load logger definitions from the properties file and registered loggers.
     *
     * @return list of logger definitions
     */
    private static synchronized List<LoggerDefinition> loadLoggerDefinitions() {
        if (bigDataLoggers != null) {
            return bigDataLoggers;
        }

        List<LoggerDefinition> loggers = new ArrayList<>();

        // Load from properties file
        Properties props = new Properties();
        try (InputStream is = BigDataLogConfig.class.getClassLoader().getResourceAsStream(LOGGING_PROPERTIES_FILE)) {
            if (is != null) {
                props.load(is);
                logger.debug("Loaded logging configuration from {}", LOGGING_PROPERTIES_FILE);

                for (String propName : props.stringPropertyNames()) {
                    if (propName.startsWith(LOGGER_PREFIX)) {
                        String loggerName = propName.substring(LOGGER_PREFIX.length());
                        String levelStr = props.getProperty(propName);

                        try {
                            Level level = Level.toLevel(levelStr, Level.INFO);
                            loggers.add(new LoggerDefinition(loggerName, level));
                            logger.debug("Loaded logger from properties: {} = {}", loggerName, level);
                        } catch (Exception e) {
                            logger.warn("Invalid log level '{}' for logger '{}', defaulting to INFO", levelStr, loggerName);
                            loggers.add(new LoggerDefinition(loggerName, Level.INFO));
                        }
                    }
                }
            } else {
                logger.warn("Could not find {} - using default logger configuration", LOGGING_PROPERTIES_FILE);
                // Add default loggers as fallback
                addDefaultLoggers(loggers);
            }
        } catch (IOException e) {
            logger.error("Error loading {} - using default logger configuration", LOGGING_PROPERTIES_FILE, e);
            addDefaultLoggers(loggers);
        }

        // Add registered loggers (these override properties file if duplicate)
        for (Map.Entry<String, Level> entry : registeredLoggers.entrySet()) {
            // Remove any existing definition for this logger name
            loggers.removeIf(def -> def.name.equals(entry.getKey()));
            loggers.add(new LoggerDefinition(entry.getKey(), entry.getValue()));
            logger.debug("Added registered logger: {} = {}", entry.getKey(), entry.getValue());
        }

        bigDataLoggers = loggers;
        return loggers;
    }

    /**
     * Add default logger definitions when properties file is not available.
     *
     * @param loggers list to add default loggers to
     */
    private static void addDefaultLoggers(List<LoggerDefinition> loggers) {
        loggers.add(new LoggerDefinition("com.pentaho.big.data", Level.INFO));
        loggers.add(new LoggerDefinition("org.pentaho.big.data", Level.INFO));
        loggers.add(new LoggerDefinition("org.apache.hadoop", Level.INFO));
        loggers.add(new LoggerDefinition("org.apache.hadoop.io.retry", Level.WARN));
        loggers.add(new LoggerDefinition("org.apache.hbase", Level.INFO));
        loggers.add(new LoggerDefinition("org.apache.hive", Level.INFO));
        loggers.add(new LoggerDefinition("org.apache.sqoop", Level.INFO));
        loggers.add(new LoggerDefinition("org.apache.kafka", Level.WARN));
        loggers.add(new LoggerDefinition("org.apache.spark", Level.WARN));
        loggers.add(new LoggerDefinition("com.pentaho.yarn", Level.INFO));
    }
    "PENTAHOCONSOLE";
  
  private static final String BIG_DATA_APPENDER_NAME = "big-data-execution-appender";

    private static RuntimeEnvironment detectedEnvironment = null;

    /**
     * Register a logger to be configured during initialization. This method
     * must be called before initializeBigDataLogging() is invoked.
     *
     * @param loggerName the fully qualified logger name (e.g.,
     * "org.apache.hbase")
     * @param level the log level for this logger
     * @return true if registered successfully, false if already initialized try
     * { LoggerContext context = (LoggerContext) LogManager.getContext(false);
     * Configuration config = context.getConfiguration();

     *      *// Load logger definitions from properties file and registrations
     * List<LoggerDefinition> loggerDefinitions = loadLoggerDefinitions();
     * logger.info("Loaded {} logger definitions", loggerDefinitions.size());

     *      *// Detect the runtime environment detectedEnvironment =
     * detectEnvironment(config); logger.info("Detected runtime environment:
     * {}", detectedEnvironment);

     *      *// Check if Big Data loggers are already configured in the main
     * log4j2.xml boolean loggersAlreadyConfigured =
     * checkIfLoggersConfigured(config, loggerDefinitions);

     *      *if (loggersAlreadyConfigured) { logger.info("Big Data loggers already
     * configured in main log4j2.xml - using existing configuration"); } else {
     * logger.info("Big Data loggers not found in main configuration - adding
     * programmatically"); configureBigDataLoggers(context, config,
     * loggerDefinitions); }

     *      *initialized = true;

     *      *} catch (Exception e) { logger.error("Failed to initialize Big Data
     * logging", e); initialized = true; } Load logger definitions from the
     * properties file and registered loggers.
     *
     * /**
     * Check if Big Data loggers are already configured in the Log4j2
     * configuration. A logger is considered "configured" if it has explicit
     * appender references.
     *
     * @param config the Log4j2 configuration
     * @param loggerDefinitions list of logger definitions to check
     * @return true if at least one Big Data logger has appender references
     */
    private static boolean checkIfLoggersConfigured(Configuration config, List<LoggerDefinition> loggerDefinitions) {
        for (LoggerDefinition loggerDef : loggerDefinitions) {
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

try {
              Level level = Level.toLevel(levelStr, Level.INFO);
              loggers.add(new LoggerDefinition(loggerName, level));
              logger.debug("Loaded logger from properties: {} = {}", loggerName, level);
            } catch (Exception e) {
              logger.warn("Invalid log level '{}' for logger '{}', defaulting to INFO", levelStr, loggerName);
              loggers.add(new LoggerDefinition(loggerName, Level.INFO));
            }
          }
        }
      } else {
        logger.warn("Could not find {} - using default logger configuration", LOGGING_PROPERTIES_FILE);
        // Add default loggers as fallback
        addDefaultLoggers(loggers);
      }
    } catch (IOException e) {
      logger.error("Error loading {} - using default logger configuration", LOGGING_PROPERTIES_FILE, e);
      addDefaultLoggers(loggers);
    }

    // Add registered loggers (these override properties file if duplicate)
    for (Map.Entry<String, Level> entry : registeredLoggers.entrySet()) {
      // Remove any existing definition for this logger name
      loggers.removeIf(def -> def.name.equals(entry.getKey()));
      loggers.add(new LoggerDefinition(entry.getKey(), entry.getValue()));
      logger.debug("Added registered logger: {} = {}", entry.getKey(), entry.getValue());
    }

    bigDataLoggers = loggers;
  /**
   * Programmatically configure Big Data loggers to write to application logs and console.
   *
   * @param context the LoggerContext
   * @param config  the Log4j2 configuration
   * @param loggerDefinitions list of logger definitions to configure
   */
  private static void configureBigDataLoggers(LoggerContext context, Configuration config, List<LoggerDefinition> loggerDefinitions) {
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
    for (LoggerDefinition loggerDef : loggerDefinitions) {ironment = detectEnvironment(config);
      logger.info("Detected runtime environment: {}", detectedEnvironment);

      // Check if Big Data loggers are already configured in the main log4j2.xml
      boolean loggersAlreadyConfigured = checkIfLoggersConfigured(config, loggerDefinitions);

      if (loggersAlreadyConfigured) {
        logger.info("Big Data loggers already configured in main log4j2.xml - using existing configuration");
      } else {
        logger.info("Big Data loggers not found in main configuration - adding programmatically");
        configureBigDataLoggers(context, config, loggerDefinitions);
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
   * @param loggerDefinitions list of logger definitions to check
   * @return true if at least one Big Data logger has appender references
   */
  private static boolean checkIfLoggersConfigured(Configuration config, List<LoggerDefinition> loggerDefinitions) {
    for (LoggerDefinition loggerDef : loggerDefinitions) {
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
   * @param loggerDefinitions list of logger definitions to configure
   */
  private static void configureBigDataLoggers(LoggerContext context, Configuration config, List<LoggerDefinition> loggerDefinitions) {
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
    for (LoggerDefinition loggerDef : loggerDefinitions) {
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

        if (!loggerConfig.getAppenders().containsKey(existingAppenderName)
                && !loggerConfig.getAppenders().containsKey(BIG_DATA_APPENDER_NAME)) {
            /**
             * Resets the initialization flag (mainly for testing purposes).
             * Also clears registered loggers and cached logger definitions.
             */
protected static void resetInitialization() {
    initialized = false;
    registeredLoggers.clear();
    bigDataLoggers = null;
  }   }
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
    
ic static Logger getBigDataLogger(Class<?> clazz) {
     Ensure initialization before returning logger
     (!initialized) {
      initializeBigDataLogging();

    return LogManager.getLogger(clazz);
                
                
                
                a logge
                
                m nam
                rn conf
                
  public static Logger getBigDataLogger(String name) {
nsure initialization before returning logger
    if (!initialized) {
      initializeBigDataLogging();
    }
             LogManager.getLogger(name);
  }

  /**
    hecks if the Big Data logging has been initialized.
   *
   * @return true if initialized, false otherwise
   */
                tatic boolean isInitialized() {
rn initialized;
  }
                 
            /**
             * Resets the initialization flag (mainly for testing purposes).
             * Also clears registered loggers and cached logger definitions.
             */
protected static void resetInitialization() {
    initialized = false;
    registeredLoggers.clear();
    bigDataLoggers = null;
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

 
 
 
                    
    LoggerDefinition(String name, Level level) {
      this.name = name;
      this.level = level;
    }
  }
}

 
 
 
                    