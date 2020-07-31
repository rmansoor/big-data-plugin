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

import com.amazonaws.auth.AWSCredentials;
import org.pentaho.di.core.encryption.Encr;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;

public class PentahoVFSAwareCredentials implements AWSCredentials {
  public static final String ACCESS_KEY = "accessKey";
  public static final String SECRET_KEY = "secretKey";
  public static final String ENDPOINT_URL = "endpoint";
  public static final String API_SIGNATURE = "signatureVersion";
  public static final String PATH_STYLE_ACCESS = "pathStyleAccess";
  private static final Logger logger = LoggerFactory.getLogger( LumadaPropertiesCredentials.class );
  private final String accessKey;
  private final String secretAccessKey;
  private final String endpointUrl;
  private final String apiSignature;
  private final boolean pathStyleAccess;

  public PentahoVFSAwareCredentials( Map<String, String> properties ) throws IOException {
    if ( properties == null || properties.size() == 0 ) {
      throw new FileNotFoundException( "Properties is null" );
    } else {
      if ( properties.get( ACCESS_KEY ) == null || properties.get( SECRET_KEY ) == null
          || properties.get( ENDPOINT_URL ) == null || properties.get( API_SIGNATURE ) == null ) {
        throw new IllegalArgumentException(
            "The properties doesn't contain the expected properties 'accessKey' and 'secretKey'." );
      }

      this.accessKey = Encr.decryptPasswordOptionallyEncrypted( properties.get( ACCESS_KEY ) );
      this.secretAccessKey = Encr.decryptPasswordOptionallyEncrypted( properties.get( SECRET_KEY ) );
      this.endpointUrl = properties.get( ENDPOINT_URL );
      this.apiSignature = properties.get( API_SIGNATURE );
      this.pathStyleAccess =
          properties.get( PATH_STYLE_ACCESS ) != null && ( properties.get( PATH_STYLE_ACCESS ).equalsIgnoreCase( "yes" )
              || properties.get( PATH_STYLE_ACCESS ).equalsIgnoreCase( "true" ) );
    }
  }

  public String getAWSAccessKeyId() {
    return this.accessKey;
  }

  public String getAWSSecretKey() {
    return this.secretAccessKey;
  }

  public String getEndpointUrl() {
    return this.endpointUrl;
  }

  public String getApiSignature() {
    return this.apiSignature;
  }

  public boolean getPathStyleAccess() {
    return this.pathStyleAccess;
  }
}
