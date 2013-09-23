/**
 * (c) Copyright 2013 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kiji.modelrepo.tools;

import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;

import com.google.common.collect.Lists;
import com.google.common.io.Files;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.kiji.express.avro.AvroModelDefinition;
import org.kiji.express.avro.AvroModelEnvironment;
import org.kiji.modelrepo.KijiModelRepository;
import org.kiji.modelrepo.ModelArtifact;
import org.kiji.modelrepo.TestUtils;
import org.kiji.schema.EntityId;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableWriter;
import org.kiji.schema.tools.BaseTool;
import org.kiji.schema.tools.KijiToolTest;
import org.kiji.schema.util.FromJson;
import org.kiji.schema.util.ProtocolVersion;

public class TestDeployModelRepoTool extends KijiToolTest {

  private Kiji mKiji = null;
  private File mTempDir = null;

  @Before
  public void setupModelRepo() throws Exception {
    mKiji = createTestKiji();
    mTempDir = Files.createTempDir();

    mTempDir.deleteOnExit();
    KijiModelRepository.install(mKiji, mTempDir.toURI());
  }

  private List<String> getBaselineArgs() {
    return Lists.newArrayList(new String[] {
        "--definition=src/test/resources/org/kiji/samplelifecycle/model_definition.json"
        , "--environment=src/test/resources/org/kiji/samplelifecycle/model_environment.json"
        , "--message=Uploading Artifact"
        , "--kiji=" + mKiji.getURI().toString()
        ,
    });
  }

  private static String makeDependencyString(List<File> inputDeps) {
    StringBuilder builder = new StringBuilder();
    for (File f : inputDeps) {
      builder.append(f.getAbsolutePath());
      builder.append(":");
    }
    return builder.toString();
  }

  // 1) Test deploying a new model not specifying a version to a blank table.
  @Test
  public void testShouldDeployNewModelToBlankTable() throws Exception {
    // 1) Setup the artifact
    List<File> dependencies = TestUtils.getDependencies(5);
    File artifactJar = TestUtils.createFakeJar("artifact");
    String groupName = "org.kiji.test";
    String artifactName = "sample_model";
    List<String> args = Lists.newArrayList();
    args.add(groupName);
    args.add(artifactName);
    args.add(artifactJar.getAbsolutePath());
    args.add("--deps=" + makeDependencyString(dependencies));
    args.add("--deps-resolver=raw");
    args.addAll(getBaselineArgs());

    int status = runTool(new DeployModelRepoTool(), args.toArray(new String[0]));
    Assert.assertEquals(BaseTool.SUCCESS, status);

    String expectedLocation = "org/kiji/test/sample_model/0.0.1/sample_model-0.0.1.war";
    // Check that the artifact was deployed
    File deployedFile = new File(mTempDir, expectedLocation);
    Assert.assertTrue(deployedFile.exists());

    // Check some attributes of the table.
    KijiModelRepository repo = KijiModelRepository.open(mKiji);

    KijiRowData lifeCycleRow = repo.getModelLifeCycle(groupName, artifactName,
        ProtocolVersion.parse("0.0.1"));
    Assert.assertTrue(lifeCycleRow
        .containsColumn(ModelArtifact.MODEL_REPO_FAMILY, ModelArtifact.LOCATION_KEY));
    Assert.assertEquals("Uploading Artifact",
        lifeCycleRow
            .getMostRecentValue(ModelArtifact.MODEL_REPO_FAMILY, ModelArtifact.MESSAGES_KEY)
            .toString());
    String relativeLocation = lifeCycleRow
        .getMostRecentValue(ModelArtifact.MODEL_REPO_FAMILY, ModelArtifact.LOCATION_KEY).toString();
    Assert.assertEquals("org/kiji/test/sample_model/0.0.1/sample_model-0.0.1.war",
        relativeLocation);

    repo.close();
  }

  // 2) Test deploying a new model specifying a version to a blank table (and message).
  @Test
  public void testShouldDeployNewModelWithVersionToBlankTable() throws Exception {
    // 1) Setup the artifact
    List<File> dependencies = TestUtils.getDependencies(5);
    File artifactJar = TestUtils.createFakeJar("artifact");
    String groupName = "org.kiji.test";
    String artifactName = "sample_model";
    List<String> args = Lists.newArrayList();
    args.add(groupName);
    args.add(artifactName);
    args.add(artifactJar.getAbsolutePath());
    args.add("--deps=" + makeDependencyString(dependencies));
    args.add("--deps-resolver=raw");
    args.add("--version=0.0.1");
    args.addAll(getBaselineArgs());

    int status = runTool(new DeployModelRepoTool(), args.toArray(new String[0]));
    Assert.assertEquals(BaseTool.SUCCESS, status);

    String expectedLocation = "org/kiji/test/sample_model/0.0.1/sample_model-0.0.1.war";
    // Check that the artifact was deployed
    File deployedFile = new File(mTempDir, expectedLocation);
    Assert.assertTrue(deployedFile.exists());

    // Check some attributes of the table.
    KijiModelRepository repo = KijiModelRepository.open(mKiji);

    KijiRowData lifeCycleRow = repo.getModelLifeCycle(groupName, artifactName,
        ProtocolVersion.parse("0.0.1"));
    Assert.assertTrue(lifeCycleRow.containsColumn(
        ModelArtifact.MODEL_REPO_FAMILY,
        ModelArtifact.LOCATION_KEY));
    Assert.assertEquals("Uploading Artifact",
        lifeCycleRow
            .getMostRecentValue(ModelArtifact.MODEL_REPO_FAMILY, ModelArtifact.MESSAGES_KEY)
            .toString());
    String relativeLocation =
        lifeCycleRow
            .getMostRecentValue(ModelArtifact.MODEL_REPO_FAMILY, ModelArtifact.LOCATION_KEY)
            .toString();
    Assert.assertEquals("org/kiji/test/sample_model/0.0.1/sample_model-0.0.1.war",
        relativeLocation);

    repo.close();
  }

  // 3) Test deploying a new model to a populated table not specifying the version
  @Test
  public void testShouldDeployNewModelToPopulatedTable() throws Exception {
    // 1) Populate the table with some stuff
    KijiTable table = mKiji.openTable(KijiModelRepository.MODEL_REPO_TABLE_NAME);
    KijiTableWriter writer = table.openTableWriter();
    EntityId eid = table.getEntityId("org.kiji.test.sample_model", "1.0.0");
    writer.put(eid, ModelArtifact.MODEL_REPO_FAMILY, ModelArtifact.LOCATION_KEY, "stuff");
    writer.close();
    table.release();

    // 2) Setup the artifact
    List<File> dependencies = TestUtils.getDependencies(5);
    File artifactJar = TestUtils.createFakeJar("artifact");
    String groupName = "org.kiji.test";
    String artifactName = "sample_model";
    List<String> args = Lists.newArrayList();
    args.add(groupName);
    args.add(artifactName);
    args.add(artifactJar.getAbsolutePath());
    args.add("--deps=" + makeDependencyString(dependencies));
    args.add("--deps-resolver=raw");
    args.addAll(getBaselineArgs());

    int status = runTool(new DeployModelRepoTool(), args.toArray(new String[0]));
    Assert.assertEquals(BaseTool.SUCCESS, status);

    String expectedLocation = "org/kiji/test/sample_model/1.0.1/sample_model-1.0.1.war";
    // Check that the artifact was deployed
    File deployedFile = new File(mTempDir, expectedLocation);
    Assert.assertTrue(deployedFile.exists());

    // Check some attributes of the table.
    KijiModelRepository repo = KijiModelRepository.open(mKiji);

    KijiRowData lifeCycleRow = repo.getModelLifeCycle(groupName, artifactName,
        ProtocolVersion.parse("1.0.1"));
    Assert.assertTrue(
        lifeCycleRow.containsColumn(ModelArtifact.MODEL_REPO_FAMILY, ModelArtifact.LOCATION_KEY));
    Assert.assertEquals("Uploading Artifact",
        lifeCycleRow
            .getMostRecentValue(ModelArtifact.MODEL_REPO_FAMILY, ModelArtifact.MESSAGES_KEY)
            .toString());
    String relativeLocation = lifeCycleRow
        .getMostRecentValue(ModelArtifact.MODEL_REPO_FAMILY, ModelArtifact.LOCATION_KEY).toString();
    Assert.assertEquals("org/kiji/test/sample_model/1.0.1/sample_model-1.0.1.war",
        relativeLocation);

    repo.close();
  }

  // 4) Test deploying a new model to a populated table specifying the version
  @Test
  public void testShouldDeployNewModelWithVersionToPopulatedTable() throws Exception {
    // 1) Populate the table with some stuff
    KijiTable table = mKiji.openTable(KijiModelRepository.MODEL_REPO_TABLE_NAME);
    KijiTableWriter writer = table.openTableWriter();
    EntityId eid = table.getEntityId("org.kiji.test.sample_model", "1.0.0");
    writer.put(eid, ModelArtifact.MODEL_REPO_FAMILY, ModelArtifact.LOCATION_KEY, "stuff");
    writer.close();
    table.release();

    // 2) Setup the artifact
    List<File> dependencies = TestUtils.getDependencies(5);
    File artifactJar = TestUtils.createFakeJar("artifact");
    String groupName = "org.kiji.test";
    String artifactName = "sample_model";
    List<String> args = Lists.newArrayList();
    args.add(groupName);
    args.add(artifactName);
    args.add(artifactJar.getAbsolutePath());
    args.add("--deps=" + makeDependencyString(dependencies));
    args.add("--deps-resolver=raw");
    args.add("--version=1.0.1");
    args.addAll(getBaselineArgs());

    int status = runTool(new DeployModelRepoTool(), args.toArray(new String[0]));
    Assert.assertEquals(BaseTool.SUCCESS, status);

    String expectedLocation = "org/kiji/test/sample_model/1.0.1/sample_model-1.0.1.war";
    // Check that the artifact was deployed
    File deployedFile = new File(mTempDir, expectedLocation);
    Assert.assertTrue(deployedFile.exists());

    // Check some attributes of the table.
    KijiModelRepository repo = KijiModelRepository.open(mKiji);

    KijiRowData lifeCycleRow = repo.getModelLifeCycle(groupName, artifactName,
        ProtocolVersion.parse("1.0.1"));
    Assert.assertTrue(lifeCycleRow
        .containsColumn(ModelArtifact.MODEL_REPO_FAMILY, ModelArtifact.LOCATION_KEY));
    Assert.assertEquals("Uploading Artifact",
        lifeCycleRow
            .getMostRecentValue(ModelArtifact.MODEL_REPO_FAMILY, ModelArtifact.MESSAGES_KEY)
            .toString());
    String relativeLocation = lifeCycleRow
        .getMostRecentValue(ModelArtifact.MODEL_REPO_FAMILY, ModelArtifact.LOCATION_KEY).toString();
    Assert.assertEquals("org/kiji/test/sample_model/1.0.1/sample_model-1.0.1.war",
        relativeLocation);

    repo.close();
  }

  // 5) Test deploying an existing model to a populated table specifying the version to get a
  // conflict exception.
  @Test
  public void testFailToDeployWithVersionConflict() throws Exception {
    // 1) Populate the table with some stuff
    KijiTable table = mKiji.openTable(KijiModelRepository.MODEL_REPO_TABLE_NAME);
    KijiTableWriter writer = table.openTableWriter();
    EntityId eid = table.getEntityId("org.kiji.test.sample_model", "1.0.0");
    writer.put(eid, ModelArtifact.MODEL_REPO_FAMILY, ModelArtifact.LOCATION_KEY, "stuff");
    writer.put(eid, ModelArtifact.MODEL_REPO_FAMILY, ModelArtifact.UPLOADED_KEY, true);
    writer.close();
    table.release();

    // 2) Setup the artifact
    List<File> dependencies = TestUtils.getDependencies(5);
    File artifactJar = TestUtils.createFakeJar("artifact");
    String groupName = "org.kiji.test";
    String artifactName = "sample_model";
    List<String> args = Lists.newArrayList();
    args.add(groupName);
    args.add(artifactName);
    args.add(artifactJar.getAbsolutePath());
    args.add("--deps=" + makeDependencyString(dependencies));
    args.add("--deps-resolver=raw");
    args.add("--version=1.0.0");
    args.addAll(getBaselineArgs());

    try {
      runTool(new DeployModelRepoTool(), args.toArray(new String[0]));
      Assert.fail("Deploy passed when it should have failed with a version conflict.");
    } catch (IllegalArgumentException iae) {
      Assert.assertEquals("Error Version 1.0.0 exists.", iae.getMessage());
    }
  }

  // 6) Test deploying an artifact that doesn't exist?
  @Test
  public void testFailToDeployWhenArtifactDoesntExist() throws Exception {
    // 2) Setup the artifact
    List<File> dependencies = TestUtils.getDependencies(5);
    String groupName = "org.kiji.test";
    String artifactName = "sample_model";

    String bogusFile = String.format("non-existant-artifact-%d.jar", System.currentTimeMillis());
    List<String> args = Lists.newArrayList();
    args.add(groupName);
    args.add(artifactName);
    args.add(bogusFile);
    args.add("--deps=" + makeDependencyString(dependencies));
    args.add("--deps-resolver=raw");
    args.add("--version=1.0.0");
    args.addAll(getBaselineArgs());

    try {
      runTool(new DeployModelRepoTool(), args.toArray(new String[0]));
      Assert.fail("Deploy passed when it should have failed with a version conflict.");
    } catch (IllegalArgumentException iae) {
      Assert.assertEquals("Error: " + bogusFile + " does not exist", iae.getMessage());
    }
  }

  // 7) Test deploying a model using pom.xml to resolve deps.
  @Test
  public void testShouldDeployNewModelToBlankTableUsingPom() throws Exception {
    // 1) Setup the artifact
    File artifactJar = TestUtils.createFakeJar("artifact");
    String groupName = "org.kiji.test";
    String artifactName = "sample_model";
    List<String> args = Lists.newArrayList();
    args.add(groupName);
    args.add(artifactName);
    args.add(artifactJar.getAbsolutePath());
    args.add("--deps=src/test/resources/pom.xml");
    args.add("--deps-resolver=maven");
    args.addAll(getBaselineArgs());

    int status = runTool(new DeployModelRepoTool(), args.toArray(new String[0]));
    Assert.assertEquals(BaseTool.SUCCESS, status);

    String expectedLocation = "org/kiji/test/sample_model/0.0.1/sample_model-0.0.1.war";
    // Check that the artifact was deployed
    File deployedFile = new File(mTempDir, expectedLocation);
    Assert.assertTrue(deployedFile.exists());

    // Check some attributes of the table.
    KijiModelRepository repo = KijiModelRepository.open(mKiji);

    KijiRowData lifeCycleRow = repo.getModelLifeCycle(groupName, artifactName,
        ProtocolVersion.parse("0.0.1"));
    Assert.assertTrue(
        lifeCycleRow.containsColumn(ModelArtifact.MODEL_REPO_FAMILY, ModelArtifact.LOCATION_KEY));
    Assert.assertEquals("Uploading Artifact",
        lifeCycleRow
            .getMostRecentValue(ModelArtifact.MODEL_REPO_FAMILY, ModelArtifact.MESSAGES_KEY)
            .toString());
    String relativeLocation = lifeCycleRow
        .getMostRecentValue(ModelArtifact.MODEL_REPO_FAMILY, ModelArtifact.LOCATION_KEY).toString();
    Assert.assertEquals("org/kiji/test/sample_model/0.0.1/sample_model-0.0.1.war",
        relativeLocation);
    repo.close();

    JarInputStream jarIs = new JarInputStream(new FileInputStream(deployedFile));
    JarEntry entry = jarIs.getNextJarEntry();
    int dependentJarsFound = 0;
    while (entry != null) {
      if (entry.getName().contains(".jar")) {
        dependentJarsFound++;
      }
      entry = jarIs.getNextJarEntry();
    }
    jarIs.close();
    Assert.assertEquals(5, dependentJarsFound);
  }

  //------------------------------------------------------------------------------------------------
  // Test concurrent deployment

  private static final String EXPECTED_DEPLOYMENT_ERROR = "Error Version 1.0.0 exists.";

  /** Number of threads which will engage in a race to deploy. */
  private static final int NUMBER_OF_DEPLOY_THREADS = 4;

  @Test
  public void testConcurrentDeploy() throws IOException, InterruptedException {
    final KijiModelRepository modelRepository = KijiModelRepository.open(mKiji);
    final ExecutorService service = Executors.newFixedThreadPool(NUMBER_OF_DEPLOY_THREADS);
    final List<Future<String>> listOfDeployThreads = Lists.newArrayList();

    // Deploy threads.
    for (int i = 0; i < NUMBER_OF_DEPLOY_THREADS; i++) {
      listOfDeployThreads.add(service.submit(new DeployModelLifecycle(modelRepository)));
    }

    int exceptionsCount = 0;
    try {
      // Wait for threads to complete and check their result.
      for (final Future<String> thread : listOfDeployThreads) {
        final String result = thread.get();
        if (!"".equals(result)) {
          assertEquals("Exception must always be version conflict.",
              EXPECTED_DEPLOYMENT_ERROR, result);
          exceptionsCount++;
        }
      }
      assertEquals(
          "Number of threads failing to deploy are 1 less than the number of threads started.",
          NUMBER_OF_DEPLOY_THREADS - 1,
          exceptionsCount);
    } catch (final Exception e) {
      // There should have been no exception.
      Assert.fail("Some test-unrelated exception occured.");
    }

    service.shutdownNow();
  }

  /**
   * Thread which deploys fake model to ["org.kiji.fake.project", "1.0.0"]
   * in the model repository table.
   */
  private class DeployModelLifecycle implements Callable<String> {
    private KijiModelRepository mModelRepository;

    /**
     * Construct thread with parameter for how long the thread should wait
     * in the critical section of the deploy method.
     *
     * @param modelRepository connection to the model repository.
     */
    public DeployModelLifecycle(final KijiModelRepository modelRepository) {
      mModelRepository = modelRepository;
    }

    /** {@inheritDoc} */
    @Override
    public String call() {
      try {
        deployFakeModelLifecycle();
      } catch (final Exception e) {
        return e.getMessage();
      }
      return "";
    }

    /**
     * Read fake definition, environment, and deploy fake model.
     *
     * @throws Exception if the fake model could not be deployed to the model repository.
     */
    public void deployFakeModelLifecycle() throws Exception {
      // Read AvroModelDefinition
      final File definitionFile =
          new File("src/test/resources/org/kiji/samplelifecycle/model_definition.json");
      final BufferedReader definitionReader = new BufferedReader(new FileReader(definitionFile));
      String line;
      String definitionJson = "";
      while ((line = definitionReader.readLine()) != null) {
        definitionJson += line;
      }
      definitionReader.close();
      final AvroModelDefinition modelDefinition = (AvroModelDefinition)
          FromJson.fromJsonString(definitionJson, AvroModelDefinition.SCHEMA$);

      // Read AvroModelEnvironment
      final File environmentFile =
          new File("src/test/resources/org/kiji/samplelifecycle/model_environment.json");
      final BufferedReader environmentReader = new BufferedReader(new FileReader(environmentFile));
      String environmentJson = "";
      while ((line = environmentReader.readLine()) != null) {
        environmentJson += line;
      }
      environmentReader.close();
      final AvroModelEnvironment modelEnvironment = (AvroModelEnvironment)
          FromJson.fromJsonString(environmentJson, AvroModelEnvironment.SCHEMA$);

      // Create artifactFile
      final File artifactFile = File.createTempFile("artifact", ".jar");

      // Deploy fake mode lifecycle
      mModelRepository.deployModelLifecycle("org.kiji.fake",
          "project",
          ProtocolVersion.parse("1.0.0"),
          artifactFile,
          Lists.<File>newArrayList(),
          modelDefinition,
          modelEnvironment,
          false,
          "First deployment");
    }
  }
}
