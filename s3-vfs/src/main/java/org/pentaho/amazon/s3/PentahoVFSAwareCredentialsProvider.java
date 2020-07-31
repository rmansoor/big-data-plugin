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
import com.amazonaws.auth.AWSCredentialsProvider;
import org.pentaho.di.connections.ConnectionDetails;
import org.pentaho.di.connections.ConnectionManager;
import org.pentaho.s3.vfs.S3FileProvider;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

public class PentahoVFSAwareCredentialsProvider implements AWSCredentialsProvider {

  private static final String DEFAULT_S3_CONFIG_PROPERTY = "defaultS3Config";
  private Supplier<ConnectionManager> connectionManagerSupplier = ConnectionManager::getInstance;

  private ConnectionDetails defaultConnection = null;

  public PentahoVFSAwareCredentialsProvider() {
    Optional<ConnectionDetails> connection = getConnectionDetails().stream().filter(
      connectionDetails -> connectionDetails.getProperties().get( DEFAULT_S3_CONFIG_PROPERTY ) != null
            && connectionDetails.getProperties().get( DEFAULT_S3_CONFIG_PROPERTY ).equalsIgnoreCase( "true" ) )
        .findFirst();
    if ( connection.isPresent() ) {
      defaultConnection = connection.get();
    }
  }

  public List<ConnectionDetails> getConnectionDetails() {
    return (List<ConnectionDetails>) connectionManagerSupplier.get()
        .getConnectionDetailsByScheme( S3FileProvider.SCHEME );
  }

  public AWSCredentials getCredentials() {
    try {
      if ( defaultConnection != null ) {
        return new PentahoVFSAwareCredentials( defaultConnection.getProperties() );
      } else {
        return null;
      }
    } catch ( IOException var3 ) {
      throw new SdkClientException( "Unable to load AWS credentials" );
    }
  }

  @Override public void refresh() {

  }
}
