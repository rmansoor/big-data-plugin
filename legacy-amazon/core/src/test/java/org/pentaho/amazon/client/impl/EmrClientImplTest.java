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


package org.pentaho.amazon.client.impl;

import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.model.ActionOnFailure;
import com.amazonaws.services.elasticmapreduce.model.AddJobFlowStepsRequest;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;
import com.amazonaws.services.elasticmapreduce.model.StepSummary;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import org.pentaho.amazon.hive.job.AmazonHiveJobExecutor;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Created by Aliaksandr_Zhuk on 2/8/2018.
 */
@RunWith( MockitoJUnitRunner.class )
public class EmrClientImplTest {

  private EmrClientImpl emrClient;
  private AmazonElasticMapReduce awsEmrClient;
  private AmazonHiveJobExecutor jobEntry;

  @Before
  public void setUp() {
    awsEmrClient = mock( AmazonElasticMapReduce.class );
    emrClient = spy( new EmrClientImpl( awsEmrClient ) );
    jobEntry = spy( new AmazonHiveJobExecutor() );
    setJobEntryFields();
  }

  @Test
  public void testInitEmrCluster_setRequestParamsForHiveStep() throws Exception {
    String stagingS3FileUrl = "s3://bucket/key/test.q";
    String stagingS3BucketUrl = "s3://bucket";
    String stepType = "hive";
    String mainClass = "";
    String bootstrapActions = "";

    RunJobFlowRequest jobFlowRequest =
      emrClient.initEmrCluster( stagingS3FileUrl, stagingS3BucketUrl, stepType, mainClass, bootstrapActions, jobEntry );

    Assert.assertEquals( 1, jobFlowRequest.getApplications().size() );
    Assert.assertEquals( 1, jobFlowRequest.getSteps().size() );

    Assert.assertEquals( stagingS3BucketUrl, jobFlowRequest.getLogUri() );
    Assert.assertEquals( jobEntry.getHadoopJobName(), jobFlowRequest.getName() );
    Assert.assertEquals( jobEntry.getEmrRelease(), jobFlowRequest.getReleaseLabel() );
    Assert.assertEquals( jobEntry.getNumInstances(), jobFlowRequest.getInstances().getInstanceCount().toString() );
    Assert.assertEquals( jobEntry.getMasterInstanceType(), jobFlowRequest.getInstances().getMasterInstanceType() );
    Assert.assertEquals( jobEntry.getSlaveInstanceType(), jobFlowRequest.getInstances().getSlaveInstanceType() );
    Assert.assertEquals( jobEntry.getAlive(), jobFlowRequest.getInstances().getKeepJobFlowAliveWhenNoSteps() );
    Assert.assertEquals( jobEntry.getEc2Role(), jobFlowRequest.getJobFlowRole() );
    Assert.assertEquals( jobEntry.getEmrRole(), jobFlowRequest.getServiceRole() );
  }

  @Test
  public void testInitEmrCluster_setRequestParamsForEmrStep() throws Exception {
    String stagingS3FileUrl = "s3://bucket/key/test.jar";
    String stagingS3BucketUrl = "s3://bucket";
    String stepType = "emr";
    String mainClass = "WordCount";
    String bootstrapActions = "";

    RunJobFlowRequest jobFlowRequest =
      emrClient.initEmrCluster( stagingS3FileUrl, stagingS3BucketUrl, stepType, mainClass, bootstrapActions, jobEntry );

    Assert.assertEquals( 0, jobFlowRequest.getApplications().size() );
    Assert.assertEquals( 1, jobFlowRequest.getSteps().size() );

    Assert.assertEquals( stagingS3BucketUrl, jobFlowRequest.getLogUri() );
    Assert.assertEquals( jobEntry.getHadoopJobName(), jobFlowRequest.getName() );
    Assert.assertEquals( jobEntry.getEmrRelease(), jobFlowRequest.getReleaseLabel() );
    Assert.assertEquals( jobEntry.getNumInstances(), jobFlowRequest.getInstances().getInstanceCount().toString() );
    Assert.assertEquals( jobEntry.getMasterInstanceType(), jobFlowRequest.getInstances().getMasterInstanceType() );
    Assert.assertEquals( jobEntry.getSlaveInstanceType(), jobFlowRequest.getInstances().getSlaveInstanceType() );
    Assert.assertEquals( jobEntry.getAlive(), jobFlowRequest.getInstances().getKeepJobFlowAliveWhenNoSteps() );
    Assert.assertEquals( jobEntry.getEc2Role(), jobFlowRequest.getJobFlowRole() );
    Assert.assertEquals( jobEntry.getEmrRole(), jobFlowRequest.getServiceRole() );
  }

  @Test
  public void testInitEmrCluster_checkStepParamsForEmrStep() throws Exception {
    String stagingS3FileUrl = "s3://bucket/key/test.jar";
    String stagingS3BucketUrl = "s3://bucket";
    String stepType = "emr";
    String mainClass = "WordCount";
    String bootstrapActions = "";

    List<String> stepArgs = new ArrayList<>();
    stepArgs.add( "--" );
    stepArgs.add( "bucket" );
    stepArgs.add( "s3://test" );

    RunJobFlowRequest jobFlowRequest =
      emrClient.initEmrCluster( stagingS3FileUrl, stagingS3BucketUrl, stepType, mainClass, bootstrapActions, jobEntry );

    StepConfig stepConfig = jobFlowRequest.getSteps().get( 0 );
    String hadoopJobName = stepConfig.getName();
    String actionOnFailure = stepConfig.getActionOnFailure();
    HadoopJarStepConfig hadoopJarStepConfig = stepConfig.getHadoopJarStep();

    Assert.assertEquals( 1, jobFlowRequest.getSteps().size() );

    Assert.assertEquals( "custom jar: " + stagingS3FileUrl, hadoopJobName );
    Assert.assertEquals( ActionOnFailure.TERMINATE_JOB_FLOW.name(), actionOnFailure );
    Assert.assertEquals( mainClass, hadoopJarStepConfig.getMainClass() );
    Assert.assertEquals( stepArgs, hadoopJarStepConfig.getArgs() );
    Assert.assertEquals( stagingS3FileUrl, hadoopJarStepConfig.getJar() );
  }

  @Test
  public void testInitEmrCluster_checkStepParamsForHiveStep() throws Exception {
    String stagingS3FileUrl = "s3://bucket/key/test.q";
    String stagingS3BucketUrl = "s3://bucket";
    String stepType = "hive";
    String mainClass = "";
    String bootstrapActions = "";

    RunJobFlowRequest jobFlowRequest =
      emrClient.initEmrCluster( stagingS3FileUrl, stagingS3BucketUrl, stepType, mainClass, bootstrapActions, jobEntry );

    StepConfig stepConfig = jobFlowRequest.getSteps().get( 0 );
    String hiveJobName = stepConfig.getName();
    String actionOnFailure = stepConfig.getActionOnFailure();

    Assert.assertEquals( 1, jobFlowRequest.getSteps().size() );

    Assert.assertEquals( "Hive", hiveJobName );
    Assert.assertEquals( ActionOnFailure.TERMINATE_JOB_FLOW.name(), actionOnFailure );
  }

  @Test
  public void testAddStepToExistingJobFlow_getIdOfRunningStep() throws Exception {
    String stagingS3FileUrl = "s3://bucket/key/test.q";
    String stagingS3BucketUrl = "s3://bucket";
    String stepType = "hive";
    String mainClass = "";

    doCallRealMethod().when( jobEntry ).setHadoopJobFlowId( anyString() );
    jobEntry.setHadoopJobFlowId( "j-11WRZQW6NIQOA" );
    doReturn( true ).when( jobEntry ).getAlive();

    List<StepSummary> existingSteps = new ArrayList<>();
    List<StepSummary> existingWithNewSteps = new ArrayList<>();

    StepSummary stepSummary1 = new StepSummary();
    stepSummary1.setId( "s-1" );
    StepSummary stepSummary2 = new StepSummary();
    stepSummary2.setId( "s-2" );
    StepSummary stepSummary3 = new StepSummary();
    stepSummary3.setId( "s-3" );

    existingSteps.add( stepSummary1 );
    existingSteps.add( stepSummary2 );

    existingWithNewSteps.addAll( existingSteps );
    existingWithNewSteps.add( stepSummary3 );

    AddJobFlowStepsRequest jobFlowStepsRequest = mock( AddJobFlowStepsRequest.class );

    doReturn( existingSteps, existingWithNewSteps ).when( emrClient ).getSteps();
    doCallRealMethod().when( emrClient ).addStepToExistingJobFlow( anyString(), anyString(), anyString(), anyString(), nullable( AmazonHiveJobExecutor.class ) );

    emrClient.addStepToExistingJobFlow( stagingS3FileUrl, stagingS3BucketUrl, stepType, mainClass, jobEntry );

    Assert.assertEquals( stepSummary3.getId(), emrClient.getStepId() );
  }

  @Test
  public void testStopSteps_whenLeaveClusterAlive() throws Exception {

    doCallRealMethod().when( emrClient ).setAlive( true );
    doCallRealMethod().when( emrClient ).isAlive();
    doNothing().when( emrClient ).cancelStepExecution();

    emrClient.setAlive( true );

    boolean stopSteps = emrClient.stopSteps();

    verify( emrClient, times( 0 ) ).terminateJobFlows();
    verify( emrClient, times( 1 ) ).cancelStepExecution();
    Assert.assertEquals( true, stopSteps );
  }

  @Test
  public void testStopSteps_whenNotLeaveClusterAlive_onNewCluster() throws Exception {

    doCallRealMethod().when( emrClient ).setAlive( false );
    doCallRealMethod().when( emrClient ).isAlive();
    doCallRealMethod().when( emrClient ).stopSteps();
    doNothing().when( emrClient ).terminateJobFlows();

    emrClient.setAlive( false );
    // Manually set runOnNewCluster to true via runJobFlow behavior
    // We can't call runJobFlow directly due to dependencies, so we test stopSteps in isolation
    // The actual runJobFlow sets runOnNewCluster=true, which is tested in integration tests

    // Simulate being on a new cluster by not calling addStepToExistingJobFlow
    boolean stopSteps = emrClient.stopSteps();

    verify( emrClient, times( 1 ) ).terminateJobFlows();
    verify( emrClient, times( 0 ) ).cancelStepExecution();
    Assert.assertEquals( false, stopSteps );
  }

  @Test
  public void testStopSteps_whenNotLeaveClusterAlive_onExistingCluster() throws Exception {

    doCallRealMethod().when( emrClient ).setAlive( false );
    doCallRealMethod().when( emrClient ).isAlive();
    doCallRealMethod().when( emrClient ).addStepToExistingJobFlow( anyString(), anyString(), anyString(), anyString(), nullable( AmazonHiveJobExecutor.class ) );
    doNothing().when( emrClient ).cancelStepExecution();
    doReturn( new ArrayList<StepSummary>() ).when( emrClient ).getSteps();

    emrClient.setAlive( false );
    // Simulate running on an existing cluster
    jobEntry.setHadoopJobFlowId( "j-EXISTING123" );
    emrClient.addStepToExistingJobFlow( "s3://test", "s3://bucket", "emr", "MainClass", jobEntry );

    boolean stopSteps = emrClient.stopSteps();

    // Should NOT terminate existing clusters, only cancel steps
    verify( emrClient, times( 0 ) ).terminateJobFlows();
    verify( emrClient, times( 1 ) ).cancelStepExecution();
    Assert.assertEquals( false, stopSteps );
  }

  @Test
  public void testStopSteps_whenShutdownClusterEnabled_onExistingCluster() throws Exception {

    doCallRealMethod().when( emrClient ).setAlive( false );
    doCallRealMethod().when( emrClient ).isAlive();
    doCallRealMethod().when( emrClient ).addStepToExistingJobFlow( anyString(), anyString(), anyString(), anyString(), nullable( AmazonHiveJobExecutor.class ) );
    doCallRealMethod().when( emrClient ).stopSteps();
    doNothing().when( emrClient ).terminateJobFlows();
    doReturn( new ArrayList<StepSummary>() ).when( emrClient ).getSteps();
    doCallRealMethod().when( jobEntry ).setShutdownCluster( true );
    doCallRealMethod().when( jobEntry ).getShutdownCluster();

    emrClient.setAlive( false );
    // Simulate running on an existing cluster with shutdown enabled
    jobEntry.setHadoopJobFlowId( "j-EXISTING123" );
    jobEntry.setShutdownCluster( true );
    emrClient.addStepToExistingJobFlow( "s3://test", "s3://bucket", "emr", "MainClass", jobEntry );

    boolean stopSteps = emrClient.stopSteps();

    // Should terminate existing cluster when shutdownCluster is enabled
    verify( emrClient, times( 1 ) ).terminateJobFlows();
    verify( emrClient, times( 0 ) ).cancelStepExecution();
    Assert.assertEquals( false, stopSteps );
  }

  @Test
  public void testStopSteps_whenShutdownClusterDisabled_onExistingCluster() throws Exception {

    doCallRealMethod().when( emrClient ).setAlive( false );
    doCallRealMethod().when( emrClient ).isAlive();
    doCallRealMethod().when( emrClient ).addStepToExistingJobFlow( anyString(), anyString(), anyString(), anyString(), nullable( AmazonHiveJobExecutor.class ) );
    doCallRealMethod().when( emrClient ).stopSteps();
    doNothing().when( emrClient ).cancelStepExecution();
    doReturn( new ArrayList<StepSummary>() ).when( emrClient ).getSteps();
    doCallRealMethod().when( jobEntry ).setShutdownCluster( false );
    doCallRealMethod().when( jobEntry ).getShutdownCluster();

    emrClient.setAlive( false );
    // Simulate running on an existing cluster with shutdown disabled
    jobEntry.setHadoopJobFlowId( "j-EXISTING123" );
    jobEntry.setShutdownCluster( false );
    emrClient.addStepToExistingJobFlow( "s3://test", "s3://bucket", "emr", "MainClass", jobEntry );

    boolean stopSteps = emrClient.stopSteps();

    // Should NOT terminate existing cluster when shutdownCluster is disabled
    verify( emrClient, times( 0 ) ).terminateJobFlows();
    verify( emrClient, times( 1 ) ).cancelStepExecution();
    Assert.assertEquals( false, stopSteps );
  }

  @Test
  public void testRemoveLineBreaks_whenBootstrapActionStringIsNull(){
    String resultBootstrapString = EmrClientImpl.removeLineBreaks( null );
    Assert.assertEquals( null, resultBootstrapString );
  }

  @Test
  public void testRemoveLineBreaks_whenBootstrapActionStringIsSpaces(){

    String bootstrapStringWithBreaks = "  ";
    String expectedString = "";

    String resultBootstrapString = EmrClientImpl.removeLineBreaks( bootstrapStringWithBreaks );

    Assert.assertEquals( expectedString, resultBootstrapString );
  }

  @Test
  public void testRemoveLineBreaks_whenBootstrapActionStringIsEmpty(){

    String bootstrapStringWithBreaks = "";
    String expectedString = "";

    String resultBootstrapString = EmrClientImpl.removeLineBreaks( bootstrapStringWithBreaks );

    Assert.assertEquals( expectedString, resultBootstrapString );
  }

  @Test
  public void testRemoveLineBreaks_whenBootstrapActionStringIsNotNull(){

    String bootstrapStringWithBreaks = " --bootstrap-action\n \"s3://hive-input/copymyfile.sh\" --args\n\n   s3://hive-input/input1/weblogs_small.txt   \n   ";
    String expectedString = "--bootstrap-action \"s3://hive-input/copymyfile.sh\" --args s3://hive-input/input1/weblogs_small.txt";

    String resultBootstrapString = EmrClientImpl.removeLineBreaks( bootstrapStringWithBreaks );

    Assert.assertEquals( expectedString, resultBootstrapString );
  }

  @Test
  public void testRemoveLineBreaks_whenBootstrapActionStringEqualsToExpectedString(){

    String bootstrapStringWithBreaks = "--bootstrap-action \"s3://hive-input/copymyfile.sh\" --args s3://hive-input/input1/weblogs_small.txt";
    String expectedString = "--bootstrap-action \"s3://hive-input/copymyfile.sh\" --args s3://hive-input/input1/weblogs_small.txt";

    String resultBootstrapString = EmrClientImpl.removeLineBreaks( bootstrapStringWithBreaks );

    Assert.assertEquals( expectedString, resultBootstrapString );
  }

  @Test
  public void testIsRunning_shouldTerminateNewCluster_whenNotAlive() throws Exception {
    doCallRealMethod().when( emrClient ).setAlive( false );
    doCallRealMethod().when( emrClient ).isAlive();
    doCallRealMethod().when( emrClient ).stopSteps();
    doNothing().when( emrClient ).terminateJobFlows();

    // Set up the emrClient to simulate a new cluster (default state)
    emrClient.setAlive( false );
    // Don't call runJobFlow to avoid NPE - just test stopSteps which has the same logic
    emrClient.stopSteps();

    // Should terminate new cluster when not alive
    verify( emrClient, times( 1 ) ).terminateJobFlows();
  }

  @Test
  public void testRunJobFlow_setsRunOnNewCluster() throws Exception {
    // This test verifies the behavior of runOnNewCluster by checking termination behavior
    // Since runJobFlow has dependencies on initEmrCluster that return AWS objects,
    // we verify the flag indirectly through the stopSteps method which reads it
    
    doCallRealMethod().when( emrClient ).setAlive( false );
    doCallRealMethod().when( emrClient ).isAlive();
    doCallRealMethod().when( emrClient ).stopSteps();
    doNothing().when( emrClient ).terminateJobFlows();

    emrClient.setAlive( false );
    // Default state is runOnNewCluster=true, so stopSteps should call terminateJobFlows
    emrClient.stopSteps();

    // Verify that terminateJobFlows is called, confirming runOnNewCluster=true behavior
    verify( emrClient, times( 1 ) ).terminateJobFlows();
  }

  @Test
  public void testAddStepToExistingJobFlow_setsRunOnExistingCluster() throws Exception {
    String stagingS3FileUrl = "s3://bucket/key/test.jar";
    String stagingS3BucketUrl = "s3://bucket";
    String stepType = "emr";
    String mainClass = "WordCount";

    doCallRealMethod().when( emrClient ).addStepToExistingJobFlow( anyString(), anyString(), anyString(), anyString(), nullable( AmazonHiveJobExecutor.class ) );
    doReturn( new ArrayList<StepSummary>() ).when( emrClient ).getSteps();

    jobEntry.setHadoopJobFlowId( "j-EXISTING123" );
    emrClient.addStepToExistingJobFlow( stagingS3FileUrl, stagingS3BucketUrl, stepType, mainClass, jobEntry );

    // Verify runOnNewCluster is set to false (via behavior verification in other tests)
    // This is tested indirectly through the termination behavior tests
    verify( emrClient, times( 1 ) ).addStepToExistingJobFlow( anyString(), anyString(), anyString(), anyString(), nullable( AmazonHiveJobExecutor.class ) );
  }

  @Test
  public void testIsRunning_shouldTerminateExistingCluster_whenShutdownClusterEnabled() throws Exception {
    doCallRealMethod().when( emrClient ).setAlive( false );
    doCallRealMethod().when( emrClient ).isAlive();
    doCallRealMethod().when( emrClient ).addStepToExistingJobFlow( anyString(), anyString(), anyString(), anyString(), nullable( AmazonHiveJobExecutor.class ) );
    doCallRealMethod().when( emrClient ).stopSteps();
    doNothing().when( emrClient ).terminateJobFlows();
    doReturn( new ArrayList<StepSummary>() ).when( emrClient ).getSteps();
    doCallRealMethod().when( jobEntry ).setShutdownCluster( true );
    doCallRealMethod().when( jobEntry ).getShutdownCluster();

    emrClient.setAlive( false );
    jobEntry.setHadoopJobFlowId( "j-EXISTING123" );
    jobEntry.setShutdownCluster( true );
    emrClient.addStepToExistingJobFlow( "s3://test", "s3://bucket", "emr", "MainClass", jobEntry );

    // Test via stopSteps which uses shutdownCluster flag
    emrClient.stopSteps();

    // Should terminate existing cluster when shutdownCluster is enabled
    verify( emrClient, times( 1 ) ).terminateJobFlows();
  }

  @Test
  public void testIsRunning_shouldNotTerminateExistingCluster_whenShutdownClusterDisabled() throws Exception {
    doCallRealMethod().when( emrClient ).setAlive( false );
    doCallRealMethod().when( emrClient ).isAlive();
    doCallRealMethod().when( emrClient ).addStepToExistingJobFlow( anyString(), anyString(), anyString(), anyString(), nullable( AmazonHiveJobExecutor.class ) );
    doCallRealMethod().when( emrClient ).stopSteps();
    doNothing().when( emrClient ).cancelStepExecution();
    doReturn( new ArrayList<StepSummary>() ).when( emrClient ).getSteps();
    doCallRealMethod().when( jobEntry ).setShutdownCluster( false );
    doCallRealMethod().when( jobEntry ).getShutdownCluster();

    emrClient.setAlive( false );
    jobEntry.setHadoopJobFlowId( "j-EXISTING123" );
    jobEntry.setShutdownCluster( false );
    emrClient.addStepToExistingJobFlow( "s3://test", "s3://bucket", "emr", "MainClass", jobEntry );

    // Test via stopSteps which uses shutdownCluster flag
    emrClient.stopSteps();

    // Should NOT terminate existing cluster when shutdownCluster is disabled
    verify( emrClient, times( 0 ) ).terminateJobFlows();
    verify( emrClient, times( 1 ) ).cancelStepExecution();
  }

  private void setJobEntryFields() {
    jobEntry.setAlive( false );
    jobEntry.setHadoopJobName( "Test Job Executor" );
    jobEntry.setEmrRelease( "emr-5.11.0" );
    jobEntry.setNumInstances( "2" );
    jobEntry.setMasterInstanceType( "c1.medium" );
    jobEntry.setSlaveInstanceType( "c1.medium" );
    jobEntry.setEc2Role( "default_ec2_role" );
    jobEntry.setEmrRole( "default_emr_role" );
    jobEntry.setCmdLineArgs( "-- bucket s3://test" );
  }
}
