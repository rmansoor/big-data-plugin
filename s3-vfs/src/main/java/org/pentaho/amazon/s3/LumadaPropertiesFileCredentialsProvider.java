/*!
 * Copyright 2019 - 2020 Hitachi Vantara.  All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.pentaho.amazon.s3;

import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider;
import org.pentaho.di.connections.ConnectionDetails;
import org.pentaho.di.connections.ConnectionManager;
import org.pentaho.s3.vfs.S3FileProvider;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class LumadaPropertiesFileCredentialsProvider extends ClasspathPropertiesFileCredentialsProvider {
  public static final String SLASH = "/";
  private static String defaultPropertiesFile = "AwsCredentials.properties";
  private final String credentialsFilePath;
  private Supplier<ConnectionManager> connectionManagerSupplier = ConnectionManager::getInstance;

  private Map<String, String> defaultConfig;

  public LumadaPropertiesFileCredentialsProvider() {
    this( defaultPropertiesFile ); for ( ConnectionDetails connectionDetails : getConnectionDetails() ) {
      Map<String, String> properties = connectionDetails.getProperties();
      if ( properties != null && properties.containsKey( "defaultS3Config" ) ) {
        String defaultS3Config = properties.get( "defaultS3Config" );
        if ( defaultS3Config != null && defaultS3Config.equals( "true" ) ) {
          defaultConfig = properties; break;
        }
      }
    }
  }

  public List<ConnectionDetails> getConnectionDetails() {
    return (List<ConnectionDetails>) connectionManagerSupplier.get()
        .getConnectionDetailsByScheme( S3FileProvider.SCHEME );
  }

  public LumadaPropertiesFileCredentialsProvider( String credentialsFilePath ) {
    if ( credentialsFilePath == null ) {
      throw new IllegalArgumentException( "Credentials file path cannot be null" );
    } else {
      if ( !credentialsFilePath.startsWith( "/" ) ) {
        this.credentialsFilePath = SLASH + credentialsFilePath;
      } else {
        this.credentialsFilePath = credentialsFilePath;
      }

    }
  }

  @Override
  public AWSCredentials getCredentials() {
    /*InputStream inputStream = this.getClass().getResourceAsStream( this.credentialsFilePath );
    if ( inputStream == null ) {
      throw new SdkClientException(
        "Unable to load AWS credentials from the " + this.credentialsFilePath + " file on the classpath" );
    } else {*/
      try {
        return new LumadaPropertiesCredentials( defaultConfig );
      } catch ( IOException var3 ) {
        throw new SdkClientException(
          "Unable to load AWS credentials from the " + this.credentialsFilePath + " file on the classpath", var3 );
      }
    /*}*/
  }
}
