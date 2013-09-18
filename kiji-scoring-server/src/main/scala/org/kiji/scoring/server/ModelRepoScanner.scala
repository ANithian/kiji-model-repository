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

import org.kiji.modelrepo.KijiModelRepository
import scala.collection.mutable.Map
import java.io.File
import org.eclipse.jetty.overlays.OverlayedAppProvider
import org.apache.commons.io.FilenameUtils
import org.apache.commons.io.FileUtils
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import scala.collection.JavaConversions._
import org.kiji.modelrepo.ModelArtifact
import com.google.common.io.Files
import scala.io.Source
import java.io.PrintWriter
import org.kiji.schema.Kiji
import org.kiji.schema.KijiURI
import scala.collection.mutable.ListBuffer

/**
 * Performs the actual deployment/undeployment of model lifecycles by scanning the model
 * repository for changes. The scanner will download any necessary artifacts from the
 * model repository and deploy the necessary files so that the application is available for
 * remote scoring.
 */
class ModelRepoScanner(mKijiModelRepo: KijiModelRepository, mScanIntervalSeconds: Int,
  mBaseDir: File)
  extends Runnable {

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
  private val mLifecycleToInstanceDir = Map[ModelArtifact, File]()

  /**
   *  Stores a map of model artifact locations to their corresponding template name so that
   *  instances can be properly mapped when created against a war file that has already
   *  been previously deployed. The key is the location string from the location column
   *  in the model repo and the value is the fully qualified name of the lifecycle
   *  (group.artifact-version) to which this location is mapped to.
   */
  private val mDeployedWarFiles = Map[String, String]()

  /**
   * Turns off the internal run flag to safely stop the scanning of the model repository
   * table.
   */
  def shutdown() {
    mIsRunning = false
  }

  def run(): Unit = {
    while (mIsRunning) {
      LOG.debug("Scanning model repository for changes...")
      checkForUpdates
      Thread.sleep(mScanIntervalSeconds * 1000)
    }
  }

  def checkForUpdates() {
    val currentLifecycles = getAllEnabledLifecycles
    val toRemove = ListBuffer[ModelArtifact]()

    // Detect changes (add or remove)
    for ((lifeCycle, location) <- mLifecycleToInstanceDir) {
      if (currentLifecycles.contains(lifeCycle)) {
        currentLifecycles.remove(lifeCycle)
      } else {
        // We know that lifeCycle doesn't exist in the set of currently enabled
        // lifecycles so it's an undeploy
        LOG.info("Undeploying lifecycle " + lifeCycle.getFullyQualifiedModelName() +
          " location = " + location)
        FileUtils.deleteDirectory(location)
        toRemove.add(lifeCycle)
      }
    }
    toRemove.foreach(mLifecycleToInstanceDir.remove(_))

    for (lifeCycleToAdd <- currentLifecycles) {
      // These are the ones who are not in the currently enabled set of lifecycles
      // so we deploy and add
      LOG.info("Deploying artifact " + lifeCycleToAdd.getFullyQualifiedModelName())
      deployArtifact(lifeCycleToAdd)
    }
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

    // Populate a map of the various placeholder values to substitute in files
    val templateParamValues = Map[String, String]()
    templateParamValues.put(MODEL_ARTIFACT, artifact.getArtifactName())
    templateParamValues.put(MODEL_GROUP, artifact.getGroupName())
    templateParamValues.put(MODEL_NAME, artifact.getFullyQualifiedModelName())
    templateParamValues.put(MODEL_VERSION, artifact.getModelVersion().toString())
    templateParamValues.put(MODEL_REPO_URI, mKijiModelRepo.getURI().toString())
    templateParamValues.put(CONTEXT_PATH, (artifact.getGroupName() + "/" +
      artifact.getArtifactName()).replace('.', '/') + "/" + artifact.getModelVersion().toString())

    if (mDeployedWarFiles.contains(artifact.getLocation())) {
      // 1) Create a new instance and done.
      createNewInstance(artifact, mDeployedWarFiles.get(artifact.getLocation()).get,
        templateParamValues)
    } else {
      // If not:
      // 1) Download the artifact to a temporary location
      val artifactFile = File.createTempFile("artifact", "war")
      val finalArtifactName = artifact.getFullyQualifiedModelName() + ".war"

      artifact.downloadArtifact(artifactFile)

      // 2) Create a new Jetty template to map to the war file
      // Template is (fullyQualifiedName=warFileBase)
      val templateDirName = String.format("%s=%s", fullyQualifiedName,
        artifact.getFullyQualifiedModelName());

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
  private def createNewInstance(artifact: ModelArtifact, templateName: String,
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
  private def translateFile(filePath: String, targetFile: File,
    bookmarkParams: Map[String, String]) {
    val fileStream = Source.fromInputStream(getClass.getResourceAsStream(filePath))
    val fileWriter = new PrintWriter(targetFile)
    for (line <- fileStream.getLines) {
      val newLine = if (line.matches(".*?%[A-Z_]+%.*?")) {
        bookmarkParams.foldLeft(line) { (result, currentKV) =>
          val (key, value) = currentKV
          result.replace("%" + key + "%", value)
        }
      } else {
        line
      }
      fileWriter.println(newLine)
    }

    fileWriter.close()
  }

  /**
   * Given a directory name, will extract a tuple of template name and classifier name. In Jetty's
   * overlay deployer, the template directory and instance directory have a name formatted like:
   * (name=value OR name--value). Much of this code was borrowed from Jetty and converted
   * to Scala with some names changed for readability purposes.
   * <br/><br/>
   * For templates, "name" is the given abstract template name and "value" is the concrete
   * base name of the war file (without the .war extension).
   * <br/>
   * For instances, "name" is the name of the template from above and "value" is the concrete
   * name that can be arbitrarily given.
   *
   * @returns a 2-tuple containing the two aforementioned parts of the specified directory.
   */
  private def parseContextDirectoryName(directoryName: String): (String, String) = {
    // Borrowed from OverlayedAppProvider.ClassifiedOverlay constructor

    // Templates/Instances can be of the format "template=classifier" or "template--classifier"
    // e.g. template1=something.war and then instance1=template1
    val equalLoc = directoryName.indexOf('=');

    val separatorLoc = if (equalLoc < 0) {
      (directoryName.indexOf("--"), 2)
    } else {
      (equalLoc, 1)
    }

    val nameAndClassifier = if (separatorLoc._1 >= 0) {
      (directoryName.substring(0, separatorLoc._1),
        directoryName.substring(separatorLoc._1 + separatorLoc._2))
    } else {
      (directoryName, null)
    }

    return nameAndClassifier
  }

  /**
   * Returns all the currently enabled lifecycles from the model repository.
   * @returns all the currently enabled lifecycles from the model repository.
   */
  private def getAllEnabledLifecycles = {
    mKijiModelRepo.getModelLifecycles(null, 0, Long.MaxValue, 1, true)
  }
}
