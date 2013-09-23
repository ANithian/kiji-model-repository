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
import java.io.PrintWriter

import scala.collection.JavaConverters.asScalaSetConverter
import scala.collection.mutable.{ Map => MutableMap }
import scala.io.Source

import org.apache.commons.io.FileUtils
import org.apache.commons.io.FilenameUtils
import org.eclipse.jetty.overlays.OverlayedAppProvider
import org.slf4j.LoggerFactory
import com.google.common.io.Files

import org.kiji.modelrepo.KijiModelRepository
import org.kiji.modelrepo.ModelArtifact

/**
 * Performs the actual deployment/undeployment of model lifecycles by scanning the model
 * repository for changes. The scanner will download any necessary artifacts from the
 * model repository and deploy the necessary files so that the application is available for
 * remote scoring.
 */
class ModelRepoScanner(mKijiModelRepo: KijiModelRepository,
                       mScanIntervalSeconds: Int,
                       mBaseDir: File) extends Runnable {

  val LOG = LoggerFactory.getLogger(classOf[ModelRepoScanner])

  // Setup some constants that will get used when generating the various files to deploy
  // the lifecycle.
  val CONTEXT_PATH = "CONTEXT_PATH"
  val MODEL_NAME = "MODEL_NAME"
  val MODEL_GROUP = "MODEL_GROUP"
  val MODEL_ARTIFACT = "MODEL_ARTIFACT"
  val MODEL_VERSION = "MODEL_VERSION"
  val MODEL_REPO_URI = "MODEL_REPO_URI"

  // Some file constants
  val OVERLAY_FILE = "/org/kiji/scoring/server/instance/overlay.xml"
  val WEB_OVERLAY_FILE = "/org/kiji/scoring/server/instance/web-overlay.xml"
  val JETTY_TEMPLATE_FILE = "/org/kiji/scoring/server/template/template.xml"

  val INSTANCES_FOLDER = new File(ScoringServer.MODELS_FOLDER, OverlayedAppProvider.INSTANCES)
  val WEBAPPS_FOLDER = new File(ScoringServer.MODELS_FOLDER, OverlayedAppProvider.WEBAPPS)
  val TEMPLATES_FOLDER = new File(ScoringServer.MODELS_FOLDER, OverlayedAppProvider.TEMPLATES)

  var mIsRunning = true

  /**
   *  Stores a set of deployed lifecycles (identified by group.artifact-version) mapping
   *  to the actual folder that houses this lifecycle.
   */
  private val mLifecycleToInstanceDir = MutableMap[ModelArtifact, File]()

  /**
   *  Stores a map of model artifact locations to their corresponding template name so that
   *  instances can be properly mapped when created against a war file that has already
   *  been previously deployed. The key is the location string from the location column
   *  in the model repo and the value is the fully qualified name of the lifecycle
   *  (group.artifact-version) to which this location is mapped to.
   */
  private val mDeployedWarFiles = MutableMap[String, String]()

  /**
   * Turns off the internal run flag to safely stop the scanning of the model repository
   * table.
   */
  def shutdown() {
    mIsRunning = false
  }

  def run() {
    while (mIsRunning) {
      LOG.debug("Scanning model repository for changes...")
      checkForUpdates
      Thread.sleep(mScanIntervalSeconds * 1000)
    }
  }

  /**
   * Checks the model repository table for updates and deploys/undeploys lifecycles
   * as necessary.
   */
  def checkForUpdates() {
    val allEnabledLifecycles = getAllEnabledLifecycles

    // Split the lifecycle map into those that are already deployed and those that
    // should be undeployed (based on whether or not the currently enabled lifecycles
    // contain the deployed lifecycle.
    val (toKeep, toUndeploy) =
      mLifecycleToInstanceDir.partition(kv => allEnabledLifecycles.contains(kv._1))

    // For each lifecycle to undeploy, remove it.
    toUndeploy.map(kv => {
      val (lifeCycle, location) = kv
      LOG.info("Undeploying lifecycle " + lifeCycle.getFullyQualifiedModelName() +
        " location = " + location)
      FileUtils.deleteDirectory(location)
    })

    // Now find the set of lifecycles to add by diffing the current with the already
    // deployed and add those.
    allEnabledLifecycles.asScala.diff(toKeep.keySet).map(lifeCycle => {
      LOG.info("Deploying artifact " + lifeCycle.getFullyQualifiedModelName())
      deployArtifact(lifeCycle)
    })
  }

  /**
   * Deploys the specified model artifact by either creating a new Jetty instance or by
   * downloading the artifact and setting up a new template/instance in Jetty.
   *
   * @param artifact is the specified ModelArtifact to deploy.
   */
  private def deployArtifact(artifact: ModelArtifact) = {
    // Deploying requires a few things.
    // If we have deployed this artifact's war file before:
    val fullyQualifiedName = artifact.getFullyQualifiedModelName()

    val contextPath = (artifact.getGroupName() + "/" +
      artifact.getArtifactName()).replace('.', '/') + "/" + artifact.getModelVersion().toString()
    // Populate a map of the various placeholder values to substitute in files
    val templateParamValues = Map[String, String](
      MODEL_ARTIFACT -> artifact.getArtifactName(),
      MODEL_GROUP -> artifact.getGroupName(),
      MODEL_NAME -> artifact.getFullyQualifiedModelName(),
      MODEL_VERSION -> artifact.getModelVersion().toString(),
      MODEL_REPO_URI -> mKijiModelRepo.getURI().toString(),
      CONTEXT_PATH -> contextPath)

    if (mDeployedWarFiles.contains(artifact.getLocation())) {
      // 1) Create a new instance and done.
      createNewInstance(artifact, mDeployedWarFiles.get(artifact.getLocation()).get,
        templateParamValues)
    } else {
      // If not:
      // 1) Download the artifact to a temporary location
      val artifactFile = File.createTempFile("artifact", "war")
      // The artifact downloaded should reflect the name of the file that was uploaded
      val finalArtifactName = String.format("%s.%s", artifact.getGroupName(),
        new File(artifact.getLocation()).getName())

      artifact.downloadArtifact(artifactFile)

      // 2) Create a new Jetty template to map to the war file
      // Template is (fullyQualifiedName=warFileBase)
      val templateDirName = String.format("%s=%s", fullyQualifiedName,
        FilenameUtils.getBaseName(finalArtifactName));

      val tempTemplateDir = new File(Files.createTempDir(), "WEB-INF")
      tempTemplateDir.mkdirs()

      translateFile(JETTY_TEMPLATE_FILE, new File(tempTemplateDir, "template.xml"),
        Map[String, String]())
      artifactFile.renameTo(new File(mBaseDir,
        String.format("%s/%s", WEBAPPS_FOLDER, finalArtifactName)))

      val moveResult = tempTemplateDir.getParentFile().
        renameTo(new File(mBaseDir, String.format("%s/%s", TEMPLATES_FOLDER, templateDirName)))

      // 3) Create a new instance.
      createNewInstance(artifact, fullyQualifiedName, templateParamValues)

      mDeployedWarFiles.put(artifact.getLocation(), fullyQualifiedName)
    }
  }

  /**
   * Creates a new Jetty overlay instance.
   *
   * @param artifact is the ModelArtifact to deploy.
   * @param templateName is the name of the template to which this instance belongs.
   * @param bookmarkParams contains a map of parameters and values used when configuring
   *        the WEB-INF specific files. An example of a parameter includes the context name
   *        used when addressing this lifecycle via HTTP which is dynamically populated based
   *        on the fully qualified name of the ModelArtifact.
   */
  private def createNewInstance(artifact: ModelArtifact,
                                templateName: String,
                                bookmarkParams: Map[String, String]) = {
    // This will create a new instance by leveraging the template files on the classpath
    // and create the right directory. Maybe first create the directory in a temp location and
    // move to the right place.
    val tempInstanceDir = new File(Files.createTempDir(), "WEB-INF")
    tempInstanceDir.mkdir()

    val instanceDirName = String.format("%s=%s", templateName,
      artifact.getFullyQualifiedModelName())

    translateFile(OVERLAY_FILE, new File(tempInstanceDir, "overlay.xml"), bookmarkParams)
    translateFile(WEB_OVERLAY_FILE, new File(tempInstanceDir, "web-overlay.xml"), bookmarkParams)

    val finalInstanceDir = new File(mBaseDir,
      String.format("%s/%s", INSTANCES_FOLDER, instanceDirName))

    tempInstanceDir.getParentFile().renameTo(finalInstanceDir)

    mLifecycleToInstanceDir.put(artifact, finalInstanceDir)
    FileUtils.deleteDirectory(tempInstanceDir)
  }

  /**
   * Given a "template" WEB-INF .xml file on the classpath, this will produce a translated
   * version of the file replacing any "bookmark" values (i.e. "%PARAM%") with their actual values
   * which are dynamically generated based on the artifact being deployed.
   *
   * @param filePath is the path to the xml file on the classpath (bundled with this scoring server)
   * @param targetFile is the path where the file is going to be written.
   * @param bookmarkParams contains a map of parameters and values used when configuring
   *        the WEB-INF specific files. An example of a parameter includes the context name
   *        used when addressing this lifecycle via HTTP which is dynamically populated based
   *        on the fully qualified name of the ModelArtifact.
   */
  private def translateFile(filePath: String,
                            targetFile: File,
                            bookmarkParams: Map[String, String]) {
    val fileStream = Source.fromInputStream(getClass.getResourceAsStream(filePath))
    val fileWriter = new PrintWriter(targetFile)

    fileStream.getLines.foreach(line => {
      val newLine =
        if (line.matches(".*?%[A-Z_]+%.*?")) {
          bookmarkParams.foldLeft(line) { (result, currentKV) =>
            val (key, value) = currentKV
            result.replace("%" + key + "%", value)
          }
        } else {
          line
        }
      fileWriter.println(newLine)
    })

    fileWriter.close()
  }

  /**
   * Returns all the currently enabled lifecycles from the model repository.
   * @return all the currently enabled lifecycles from the model repository.
   */
  private def getAllEnabledLifecycles = {
    mKijiModelRepo.getModelLifecycles(null, 1, true)
  }
}
