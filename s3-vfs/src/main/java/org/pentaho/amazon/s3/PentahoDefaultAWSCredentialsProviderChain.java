package org.pentaho.amazon.s3;

import com.amazonaws.auth.AWSCredentialsProviderChain;
import com.amazonaws.auth.EC2ContainerCredentialsProviderWrapper;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.auth.SystemPropertiesCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;

public class PentahoDefaultAWSCredentialsProviderChain extends AWSCredentialsProviderChain {

  private static final PentahoDefaultAWSCredentialsProviderChain
      INSTANCE =
      new PentahoDefaultAWSCredentialsProviderChain();

  public PentahoDefaultAWSCredentialsProviderChain() {
    super( new EnvironmentVariableCredentialsProvider(), new SystemPropertiesCredentialsProvider(),
        new ProfileCredentialsProvider(), new EC2ContainerCredentialsProviderWrapper(),
        new PentahoVFSAwareCredentialsProvider() );
  }

  public static PentahoDefaultAWSCredentialsProviderChain getInstance() {
    return INSTANCE;
  }
}
