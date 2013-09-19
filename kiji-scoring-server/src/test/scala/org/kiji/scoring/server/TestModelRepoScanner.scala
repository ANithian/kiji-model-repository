package org.kiji.scoring.server

import org.scalatest.matchers._
import org.scalatest._
import scala.collection.mutable.Stack
import org.kiji.schema.Kiji
import org.kiji.schema.KijiURI
import org.kiji.schema.util.InstanceBuilder
import com.google.common.io.Files
import java.io.File
import scala.collection.mutable.Map
import com.fasterxml.jackson.databind.ObjectMapper
import scala.collection.JavaConverters._
import org.kiji.modelrepo.KijiModelRepository
import org.kiji.modelrepo.tools.DeployModelRepoTool

class TestModelRepoScanner extends FlatSpec with BeforeAndAfter {

  var mFakeKiji: Kiji = null
  var mTempHome: File = null

  before {
    val builder = new InstanceBuilder("default");
    mFakeKiji = builder.build();
    mTempHome = setupServerEnvironment
    KijiModelRepository.install(mFakeKiji, Files.createTempDir().toURI())
  }

  after {
    mFakeKiji.release()
  }

  "ModelRepoScanner" should "deploy a single lifecycle" in {
    val modelRepo = KijiModelRepository.open(mFakeKiji)
    deploySampleLifecycle("0.0.1")

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
    deploySampleLifecycle("0.0.1")

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

    deploySampleLifecycle("0.0.1")
    deploySampleLifecycle("0.0.2")

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
    assert(new File(mTempHome,"models/instances/"
    		+ "org.kiji.test.sample_model-0.0.2=org.kiji.test.sample_model-0.0.1").exists())

    assert(new File(mTempHome,"models/instances/"
        + "org.kiji.test.sample_model-0.0.2=org.kiji.test.sample_model-0.0.2").exists())

    assert(new File(mTempHome,"models/webapps/"
        + "org.kiji.test.sample_model-0.0.1.war").exists())
    assert(!new File(mTempHome,"models/webapps/"
        + "org.kiji.test.sample_model-0.0.2.war").exists())

    modelRepo.close()
  }

  def deploySampleLifecycle(version: String) {
    val groupName = "org.kiji.test"
    val artifactName = "sample_model"
    val deployTool = new DeployModelRepoTool
    // Deploy some bogus artifact. We don't care that it's not executable code yet.
    val args = List(groupName,
      artifactName,
      new File(mTempHome, "conf/configuration.json").getAbsolutePath(),
      "--kiji=" + mFakeKiji.getURI().toString(),
      "--definition=src/test/resources/org/kiji/samplelifecycle/model_definition.json",
      "--environment=src/test/resources/org/kiji/samplelifecycle/model_environment.json",
      "--production-ready=true",
      "--version=" + version,
      "--message=Uploading Artifact")

    deployTool.toolMain(args.asJava)
  }

  def setupServerEnvironment: File = {
    val tempModelDir = Files.createTempDir()
    // Create the configuration folder
    val confFolder = new File(tempModelDir, "conf")
    confFolder.mkdir()
    // Create the models folder
    new File(tempModelDir, "models/webapps").mkdirs()
    new File(tempModelDir, "models/instances").mkdirs()
    new File(tempModelDir, "models/templates").mkdirs()

    tempModelDir.deleteOnExit()

    val configMap = Map("port" -> 0, "repo_uri" -> mFakeKiji.getURI().toString(), "repo_scan_interval" -> 2)

    val mapper = new ObjectMapper()
    mapper.writeValue(new File(confFolder, "configuration.json"), configMap.asJava)

    tempModelDir
  }
}