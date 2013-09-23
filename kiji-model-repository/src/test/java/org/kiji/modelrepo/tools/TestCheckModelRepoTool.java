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
import static org.junit.Assert.assertTrue;

import java.io.File;

import com.google.common.io.Files;

import org.junit.Before;
import org.junit.Test;

import org.kiji.modelrepo.KijiModelRepository;
import org.kiji.modelrepo.ModelArtifact;
import org.kiji.modelrepo.TestUtils;
import org.kiji.schema.EntityId;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableWriter;
import org.kiji.schema.tools.BaseTool;
import org.kiji.schema.tools.KijiToolTest;

public class TestCheckModelRepoTool extends KijiToolTest {

  private Kiji mKiji = null;
  private File mTempDir = null;

  @Before
  public void setupModelRepo() throws Exception {
    mKiji = createTestKiji();
    mTempDir = Files.createTempDir();
    mTempDir.deleteOnExit();
    KijiModelRepository.install(mKiji, mTempDir.toURI());
  }

  @Test
  public void testReadFakeModelsFromModelRepo() throws Exception {
    // Set up artifacts in temporary repository.
    final File invalidJar = File.createTempFile("artifact", ".jar", mTempDir);

    // Set up model repository table manually.
    final KijiTable table = mKiji.openTable(KijiModelRepository.MODEL_REPO_TABLE_NAME);
    final KijiTableWriter writer = table.openTableWriter();
    final EntityId eid1 = table.getEntityId("org.kiji.fake.project", "1.0.0");
    final EntityId eid2 = table.getEntityId("org.kiji.fake.anotherproject", "1.0.0");
    writer.put(eid1, ModelArtifact.MODEL_REPO_FAMILY,
        ModelArtifact.LOCATION_KEY, 1L, mTempDir.toURI().relativize(invalidJar.toURI()).toString());
    writer.put(eid2, ModelArtifact.MODEL_REPO_FAMILY,
        ModelArtifact.LOCATION_KEY, "nonexistent.war");
    writer.close();
    table.release();

    // Run check model repository.
    final int status = runTool(new CheckModelRepoTool(),
        new String[] {"--kiji=" + mKiji.getURI().toString()});
    assertEquals(BaseTool.SUCCESS, status);
    assertTrue(mToolOutputLines[0].startsWith("2 issues found!"));
    assertTrue(mToolOutputLines[1]
        .startsWith("org.kiji.fake.anotherproject-1.0.0: Unable to retrieve"));
    assertTrue(mToolOutputLines[2].startsWith("org.kiji.fake.project-1.0.0: Artifact from"));
  }

  @Test
  public void testReadFakeModelsFromModelRepoDontDownload() throws Exception {
    // Set up artifacts in temporary repository.
    final File artifactJar = TestUtils.createFakeJar("artifact", ".war", mTempDir);
    final File invalidJar = File.createTempFile("artifact", ".jar", mTempDir);

    // Set up model repository table manually.
    final KijiTable table = mKiji.openTable(KijiModelRepository.MODEL_REPO_TABLE_NAME);
    final KijiTableWriter writer = table.openTableWriter();
    final EntityId eid1 = table.getEntityId("org.kiji.fake.project", "1.0.0");
    final EntityId eid2 = table.getEntityId("org.kiji.fake.anotherproject", "1.0.0");
    writer.put(eid1, ModelArtifact.MODEL_REPO_FAMILY,
        ModelArtifact.LOCATION_KEY, 1L, mTempDir.toURI().relativize(invalidJar.toURI()).toString());
    writer.put(eid1, ModelArtifact.MODEL_REPO_FAMILY,
        ModelArtifact.LOCATION_KEY, 2L,
        mTempDir.toURI().relativize(artifactJar.toURI()).toString());
    writer.put(eid2, ModelArtifact.MODEL_REPO_FAMILY,
        ModelArtifact.LOCATION_KEY, "nonexistent.war");
    writer.close();
    table.release();

    // Run check model repository.
    final int status = runTool(new CheckModelRepoTool(),
        new String[] {"--kiji=" + mKiji.getURI().toString(),
            "--download-and-validate-artifact=false", });
    assertEquals(BaseTool.SUCCESS, status);
    assertTrue(mToolOutputLines[0].startsWith("1 issues found!"));
    assertTrue(mToolOutputLines[1]
        .startsWith("org.kiji.fake.anotherproject-1.0.0: Unable to find"));
  }

  @Test
  public void testSkipNonUploadedModels() throws Exception {
    // Set up model repository table manually.
    final KijiTable table = mKiji.openTable(KijiModelRepository.MODEL_REPO_TABLE_NAME);
    final KijiTableWriter writer = table.openTableWriter();
    final EntityId eid = table.getEntityId("org.kiji.fake.anotherproject", "1.0.0");
    writer.put(eid, ModelArtifact.MODEL_REPO_FAMILY, ModelArtifact.LOCATION_KEY, "nonexistent.war");
    writer.put(eid, ModelArtifact.MODEL_REPO_FAMILY, ModelArtifact.UPLOADED_KEY, false);
    writer.close();
    table.release();

    // Run check model repository.
    final int status = runTool(new CheckModelRepoTool(),
        new String[] {"--kiji=" + mKiji.getURI().toString(),
            "--download-and-validate-artifact=false", });
    assertEquals(BaseTool.SUCCESS, status);
    assertEquals(1, mToolOutputLines.length);
    assertTrue(mToolOutputLines[0].startsWith("0 issues found!"));
  }
}
