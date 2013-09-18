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
    deploySampleLifecycle

    val scanner = new ModelRepoScanner(modelRepo, 2, mTempHome)
    scanner.checkForUpdates
    println(mTempHome.getAbsolutePath())
    val webappFile = new File(mTempHome, "models/webapps/org.kiji.test.sample_model-0.0.1.war")
    val templateDir = new File(mTempHome, "models/templates/org.kiji.test.sample_model-0.0.1=org.kiji.test.sample_model-0.0.1")
    val instanceDir = new File(mTempHome, "models/instances/org.kiji.test.sample_model-0.0.1=org.kiji.test.sample_model-0.0.1")

    assert(webappFile.exists())
    assert(templateDir.exists())
    assert(instanceDir.exists())
    modelRepo.close()
  }

  def deploySampleLifecycle() {
    val groupName = "org.kiji.test"
    val artifactName = "sample_model"
    val deployTool = new DeployModelRepoTool
    val args = List(groupName,
      artifactName,
      new File(mTempHome, "conf/configuration.json").getAbsolutePath(),
      "--kiji=" + mFakeKiji.getURI().toString(),
      "--definition=src/test/resources/org/kiji/samplelifecycle/model_definition.json",
      "--environment=src/test/resources/org/kiji/samplelifecycle/model_environment.json",
      "--production-ready=true",
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