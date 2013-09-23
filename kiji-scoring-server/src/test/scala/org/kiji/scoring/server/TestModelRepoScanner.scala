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

package org.kiji.scoring.server

import java.io.File

import scala.collection.JavaConverters.mutableMapAsJavaMapConverter
import scala.collection.JavaConverters.seqAsJavaListConverter
import scala.collection.mutable.Map

import org.kiji.modelrepo.KijiModelRepository
import org.kiji.modelrepo.tools.DeployModelRepoTool
import org.kiji.schema.Kiji
import org.kiji.schema.util.InstanceBuilder
import org.scalatest.BeforeAndAfter
import org.scalatest.FlatSpec

import org.kiji.modelrepo.KijiModelRepository
import org.kiji.modelrepo.tools.DeployModelRepoTool
import org.kiji.schema.Kiji
import org.kiji.schema.util.InstanceBuilder

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.common.io.Files
import org.kiji.scoring.server.TestUtils._

class TestModelRepoScanner extends FlatSpec with BeforeAndAfter {

  var mFakeKiji: Kiji = null
  var mTempHome: File = null

  before {
    val builder = new InstanceBuilder("default");
    mFakeKiji = builder.build();
    mTempHome = setupServerEnvironment(mFakeKiji.getURI())
    KijiModelRepository.install(mFakeKiji, Files.createTempDir().toURI())
  }

  after {
    mFakeKiji.release()
  }

  "ModelRepoScanner" should "deploy a single lifecycle" in {
    val modelRepo = KijiModelRepository.open(mFakeKiji)
    val bogusArtifact = new File(mTempHome,"conf/configuration.json").getAbsolutePath()

    deploySampleLifecycle(mFakeKiji, bogusArtifact, "0.0.1")

    val scanner = new ModelRepoScanner(modelRepo, 2, mTempHome)
    scanner.checkForUpdates
    val webappFile = new File(mTempHome, "models/webapps/org.kiji.test.sample_model-0.0.1.war")
    val templateDir = new File(mTempHome,
      "models/templates/org.kiji.test.sample_model-0.0.1=org.kiji.test.sample_model-0.0.1")
    val instanceDir = new File(mTempHome,
      "models/instances/org.kiji.test.sample_model-0.0.1=org.kiji.test.sample_model-0.0.1")

    assert(webappFile.exists())
    assert(templateDir.exists())
    assert(instanceDir.exists())
    modelRepo.close()
  }

  "ModelRepoScanner" should "undeploy a single lifecycle" in {
    val modelRepo = KijiModelRepository.open(mFakeKiji)
    val bogusArtifact = new File(mTempHome,"conf/configuration.json").getAbsolutePath()

    deploySampleLifecycle(mFakeKiji,bogusArtifact, "0.0.1")

    val scanner = new ModelRepoScanner(modelRepo, 2, mTempHome)
    scanner.checkForUpdates
    val instanceDir = new File(mTempHome,
      "models/instances/org.kiji.test.sample_model-0.0.1=org.kiji.test.sample_model-0.0.1")
    assert(instanceDir.exists())

    // For now undeploy will delete the instance directory
    val modelRepoTable = mFakeKiji.openTable(KijiModelRepository.MODEL_REPO_TABLE_NAME)
    val writer = modelRepoTable.openTableWriter()
    val eid = modelRepoTable.getEntityId("org.kiji.test.sample_model", "0.0.1")
    writer.put(eid, "model", "production_ready", false)
    writer.close()
    modelRepoTable.release()
    scanner.checkForUpdates
    assert(!instanceDir.exists())
    modelRepo.close()
  }

  "ModelRepoScanner" should "link multiple lifecycles to the same artifact" in {
    val modelRepo = KijiModelRepository.open(mFakeKiji)
    val bogusArtifact = new File(mTempHome,"conf/configuration.json").getAbsolutePath()

    deploySampleLifecycle(mFakeKiji,bogusArtifact, "0.0.1")
    deploySampleLifecycle(mFakeKiji,bogusArtifact, "0.0.2")

    // Force the location of 0.0.2 to be that of 0.0.1
    val modelRepoTable = mFakeKiji.openTable(KijiModelRepository.MODEL_REPO_TABLE_NAME)
    val writer = modelRepoTable.openTableWriter()
    val eid = modelRepoTable.getEntityId("org.kiji.test.sample_model", "0.0.2")

    writer.put(eid, "model", "location", "org/kiji/test/sample_model/0.0.1/sample_model-0.0.1.war")
    writer.close()
    modelRepoTable.release()

    val scanner = new ModelRepoScanner(modelRepo, 2, mTempHome)
    scanner.checkForUpdates
    // Check that we have two instances pointing at 0.0.2 since that is what is deployed
    // first (rows are selected in reverse time order).
    assert(new File(mTempHome, "models/instances/"
      + "org.kiji.test.sample_model-0.0.2=org.kiji.test.sample_model-0.0.1").exists())

    assert(new File(mTempHome, "models/instances/"
      + "org.kiji.test.sample_model-0.0.2=org.kiji.test.sample_model-0.0.2").exists())

    assert(new File(mTempHome, "models/webapps/"
      + "org.kiji.test.sample_model-0.0.1.war").exists())
    assert(!new File(mTempHome, "models/webapps/"
      + "org.kiji.test.sample_model-0.0.2.war").exists())

    modelRepo.close()
  }

  "ModelRepoScanner" should "undeploy an artifact after multiple lifecycles have been deployed" in {
    val modelRepo = KijiModelRepository.open(mFakeKiji)
    val bogusArtifact = new File(mTempHome,"conf/configuration.json").getAbsolutePath()

    deploySampleLifecycle(mFakeKiji,bogusArtifact, "0.0.1")
    deploySampleLifecycle(mFakeKiji,bogusArtifact, "0.0.2")

    // Force the location of 0.0.2 to be that of 0.0.1
    val modelRepoTable = mFakeKiji.openTable(KijiModelRepository.MODEL_REPO_TABLE_NAME)
    val writer = modelRepoTable.openTableWriter()
    val eid = modelRepoTable.getEntityId("org.kiji.test.sample_model", "0.0.2")

    writer.put(eid, "model", "location", "org/kiji/test/sample_model/0.0.1/sample_model-0.0.1.war")

    val scanner = new ModelRepoScanner(modelRepo, 2, mTempHome)
    scanner.checkForUpdates
    // Check that we have two instances pointing at 0.0.2 since that is what is deployed
    // first (rows are selected in reverse time order).
    assert(new File(mTempHome, "models/instances/"
      + "org.kiji.test.sample_model-0.0.2=org.kiji.test.sample_model-0.0.1").exists())

    assert(new File(mTempHome, "models/instances/"
      + "org.kiji.test.sample_model-0.0.2=org.kiji.test.sample_model-0.0.2").exists())

    assert(new File(mTempHome, "models/webapps/"
      + "org.kiji.test.sample_model-0.0.1.war").exists())
    assert(!new File(mTempHome, "models/webapps/"
      + "org.kiji.test.sample_model-0.0.2.war").exists())

    writer.put(eid, "model", "production_ready", false)
    scanner.checkForUpdates
    assert(!new File(mTempHome, "models/instances/"
      + "org.kiji.test.sample_model-0.0.2=org.kiji.test.sample_model-0.0.2").exists())

    writer.close()
    modelRepoTable.release()
    modelRepo.close()
  }
}
