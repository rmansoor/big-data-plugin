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

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.ConsoleAppender;
import org.apache.logging.log4j.core.config.AppenderRef;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.layout.PatternLayout;

/**
 * Programmatically configures Log4j2 loggers for the Big Data plugin to output to Tomcat's existing logs.
 * This class ensures that big-data component logs are visible in Tomcat's catalina.out and other standard logs
 * without creating separate log files.
 */
public class BigDataLogConfig {

  private static final String CONSOLE_APPENDER_NAME = "big-data-console-appender";
  private static final String LOG_PATTERN = "%d{yyyy-MM-dd HH:mm:ss.SSS} %-5p [%c{1.}] <%t> %m%n";
  private static final String COM_PENTAHO_BIG_DATA_LOGGER = "com.pentaho.big.data";
  private static final String ORG_PENTAHO_BIG_DATA_LOGGER = "org.pentaho.big.data";

  private static boolean initialized = false;
  private static final Logger logger = LogManager.getLogger(BigDataLogConfig.class);

  /**
   * Initializes the Big Data logging configuration programmatically.
   * This method is idempotent and can be safely called multiple times.
   * Configures loggers to output to console/stdout which Tomcat captures in catalina.out.
   */
  public static synchronized void initializeBigDataLogging() {
    if (initialized) {
      logger.debug("Big Data logging already initialized");
      return;
    }

    try {
      LoggerContext context = (LoggerContext) LogManager.getContext(false);
      Configuration config = context.getConfiguration();

      // Try to use existing console appender first (PENTAHOCONSOLE or root console)
      Appender consoleAppender = config.getAppender("PENTAHOCONSOLE");
      if (consoleAppender == null) {
        consoleAppender = config.getAppender("CONSOLE");
      }
      if (consoleAppender == null) {
        consoleAppender = config.getAppender("Console");
      }
      
      // If no existing console appender found, create one
      if (consoleAppender == null) {
        Appender existingConsoleAppender = config.getAppender(CONSOLE_APPENDER_NAME);
        if (existingConsoleAppender == null) {
          consoleAppender = createBigDataConsoleAppender(config);
          config.addAppender(consoleAppender);
          consoleAppender.start();
          logger.info("Created Big Data console appender: " + CONSOLE_APPENDER_NAME);
        } else {
          consoleAppender = existingConsoleAppender;
          logger.debug("Big Data console appender already exists: " + CONSOLE_APPENDER_NAME);
        }
      } else {
        logger.info("Using existing console appender: " + consoleAppender.getName());
      }

      // Configure loggers for big-data packages to use the console appender
      configureBigDataLogger(config, COM_PENTAHO_BIG_DATA_LOGGER, consoleAppender);
      configureBigDataLogger(config, ORG_PENTAHO_BIG_DATA_LOGGER, consoleAppender);

      // Update the configuration
      context.updateLoggers();
      
      initialized = true;
      logger.info("Big Data logging configuration initialized successfully - logs will appear in Tomcat console/catalina.out");

    } catch (Exception e) {
      logger.error("Failed to initialize Big Data logging configuration", e);
    }
  }

  /**
   * Creates a console appender for Big Data logs that outputs to stdout (captured by Tomcat).
   *
   * @param config the Log4j2 configuration
   * @return configured ConsoleAppender
   */
  private static Appender createBigDataConsoleAppender(Configuration config) {
    // Create layout
    Layout<?> layout = PatternLayout.newBuilder()
        .withConfiguration(config)
        .withPattern(LOG_PATTERN)
        .build();

    // Create the ConsoleAppender targeting stdout
    return ConsoleAppender.newBuilder()
        .withName(CONSOLE_APPENDER_NAME)
        .withLayout(layout)
        .setTarget(ConsoleAppender.Target.SYSTEM_OUT)
        .build();
  }

  /**
   * Configures a logger to use the console appender (which Tomcat captures).
   *
   * @param config         the Log4j2 configuration
   * @param loggerName     the name of the logger to configure
   * @param consoleAppender the console appender to use
   */
  private static void configureBigDataLogger(Configuration config, String loggerName, Appender consoleAppender) {
    LoggerConfig loggerConfig = config.getLoggerConfig(loggerName);
    
    // Check if this is a specific logger config or the root logger
    boolean isSpecificLogger = loggerConfig.getName().equals(loggerName);
    
    if (!isSpecificLogger) {
      // Create a new logger config for this package
      AppenderRef consoleRef = AppenderRef.createAppenderRef(consoleAppender.getName(), Level.INFO, null);
      AppenderRef[] refs = new AppenderRef[] { consoleRef };
      
      LoggerConfig newLoggerConfig = LoggerConfig.createLogger(
          true,  // additivity = true to propagate to root logger and its appenders
          Level.INFO,
          loggerName,
          "true",
          refs,
          null,
          config,
          null);
      
      newLoggerConfig.addAppender(consoleAppender, Level.INFO, null);
      config.addLogger(loggerName, newLoggerConfig);
      logger.info("Created logger configuration for: " + loggerName + " using appender: " + consoleAppender.getName());
    } else {
      // Logger already exists, just add the console appender if not already present
      if (!loggerConfig.getAppenders().containsKey(consoleAppender.getName())) {
        loggerConfig.addAppender(consoleAppender, Level.INFO, null);
        logger.info("Added console appender to existing logger: " + loggerName);
      } else {
        logger.debug("Logger already has console appender: " + loggerName);
      }
    }
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
}
