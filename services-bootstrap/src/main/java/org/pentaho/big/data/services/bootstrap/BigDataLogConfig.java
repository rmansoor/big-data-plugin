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

/**
 * Configuration class for Big Data plugin logging.
 * 
 * The Big Data plugin uses the existing Pentaho/Kettle Log4j2 configuration.
 * Logs from Big Data components (com.pentaho.big.data.*, org.pentaho.big.data.*, 
 * Hadoop, HBase, Hive, Spark, Kafka, etc.) will automatically be written to the
 * configured appenders (pdi.log and console/catalina.out) based on the main 
 * application's log4j2.xml configuration.
 * 
 * This class provides utility methods for obtaining properly configured loggers.
 */
public class BigDataLogConfig {

  private static boolean initialized = false;
  private static final Logger logger = LogManager.getLogger(BigDataLogConfig.class);

  /**
   * Initializes the Big Data logging - marks as initialized and logs a message.
   * This method is idempotent and can be safely called multiple times.
   * 
   * The actual logging configuration is managed by the main application's Log4j2
   * configuration. This method simply ensures initialization is tracked and logs
   * are properly routed through the existing logging infrastructure.
   */
  public static synchronized void initializeBigDataLogging() {
    if (initialized) {
      logger.debug("Big Data logging already initialized");
      return;
    }

    try {
      // Log initialization - the actual logging configuration is handled by
      // the main application's log4j2.xml (typically in system/karaf/etc/)
      logger.info("Big Data plugin logging initialized - using existing Pentaho Log4j2 configuration");
      logger.info("Big Data logs will appear in pdi.log and console based on root logger configuration");
      
      initialized = true;

    } catch (Exception e) {
      logger.error("Failed to initialize Big Data logging", e);
      initialized = true;
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
