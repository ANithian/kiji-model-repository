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

class ModelRepoScanner(mKijiModelRepo: KijiModelRepository, mScanIntervalSeconds: Int) extends Runnable {

  val LOG = LoggerFactory.getLogger(classOf[ModelRepoScanner])

  // Setup some constants that will get used when generating the various files to deploy
  // the lifecycle.
  val CONTEXT_PATH = "%CONTEXT_PATH%"
  val MODEL_NAME = "%MODEL_NAME%"
  val MODEL_GROUP = "%MODEL_GROUP%"
  val MODEL_ARTIFACT = "%MODEL_ARTIFACT%"
  val MODEL_VERSION = "%MODEL_VERSION%"
  val MODEL_REPO_URI = "%MODEL_REPO_URI%"

  // Some file constants
  val OVERLAY_FILE = "/org/kiji/scoring/server/instance/overlay.xml"
  val WEB_OVERLAY_FILE = "/org/kiji/scoring/server/instance/web-overlay.xml"
  val JETTY_TEMPLATE_FILE = "/org/kiji/scoring/server/template/template.xml"

  val mDeployedLifeCycles = Set[ModelArtifact]() //TODO: How to seed this from disk?
  val mDeployedTemplates = Set[String]()

  cleanupInvalidDeployments

  // Initialize the deployed lifecycles map from the model-repo
  // Then go through the model/webapps, templates, instances and ensure that stuff in there
  // is valid. Also ensure that stuff that *is* valid is actually enabled (i.e. contained in the
  // deployed lifecycles map)

  //1) Cleanup invalid deployments
  def cleanupInvalidDeployments() {
    // Get the list of webapps
    val deployedWebApps = new File(ScoringServer.MODELS_FOLDER,
      OverlayedAppProvider.WEBAPPS).listFiles()
    if (deployedWebApps != null) {
      for (webapp <- deployedWebApps) {
        // 1) Anything not a war file gets deleted.
        if (FilenameUtils.getExtension(webapp.getAbsolutePath()) != "war") {
          LOG.info("Removing invalid web application " + webapp.getAbsolutePath())
          if (webapp.isDirectory()) {
            FileUtils.deleteDirectory(webapp)
          } else {
            webapp.delete()
          }
        } else {
          // 2) For each war file we check that it is a member of the enabled lifecycles
          // The war file name is a concatenation of the group + artifact + version ("." separated)
          val webappName = FilenameUtils.getBaseName(webapp.getName())
          if (!mDeployedLifeCycles.contains(webappName)) {
            LOG.info("Removing invalid web application " + webapp.toString())
            webapp.delete()
          }
        }
      }
    }
    val deployedTemplates = new File(ScoringServer.MODELS_FOLDER,
      OverlayedAppProvider.TEMPLATES).listFiles()
    if (deployedTemplates != null) {
      for (template <- deployedTemplates) {
        // Here we need to check that the deployed templates (i.e. template_name=warfilename)
        // refers to a war file that is enabled.
        if (!template.isDirectory()) {
          LOG.info("Removing invalid template " + template.toString())
          template.delete()
        } else {
          val (templateName, classifier) = parseContextDirectoryName(template.getName())
          // Classifier has to be
          if (classifier == null || !mDeployedLifeCycles.contains(classifier)) {
            LOG.info("Removing invalid template " + template.toString())
            FileUtils.deleteDirectory(template)
          } else {
            mDeployedTemplates.add(classifier)
          }
        }
      }
    }

    val deployedInstances = new File(ScoringServer.MODELS_FOLDER,
      OverlayedAppProvider.INSTANCES).listFiles()
    if (deployedInstances != null) {
      for (instance <- deployedInstances) {
        // Here we need to check that the deployed instance (i.e. instance_name=template_name)
        // refers to a template that we know is valid (because we just checked it above)
        if (!instance.isDirectory()) {
          LOG.info("Removing invalid template " + instance.toString())
          instance.delete()
        } else {
          val (templateName, classifier) = parseContextDirectoryName(instance.getName())
          // Classifier has to be
          if (classifier == null || !mDeployedTemplates.contains(classifier)) {
            LOG.info("Removing invalid template " + instance.toString())
            FileUtils.deleteDirectory(instance)
          }
        }
      }
    }
  }

  def run(): Unit = {
    while (true) {
      val currentLifecycles = getAllEnabledLifecycles
      val toRemove = List[ModelArtifact]()

      // Detect changes (add or remove)
      for (lifeCycle <- mDeployedLifeCycles) {
        if (currentLifecycles.contains(lifeCycle)) {
          currentLifecycles.remove(lifeCycle)
        } else {
          // We know that lifeCycle doesn't exist in the set of currently enabled
          // lifecycles so it's an undeploy

          toRemove.add(lifeCycle)
        }
      }

      mDeployedLifeCycles.removeAll(toRemove)

      for (lifeCycleToAdd <- currentLifecycles) {
        // These are the ones who are not in the currently enabled set of lifecycles
        // so we deploy and add

        mDeployedLifeCycles.add(lifeCycleToAdd)
      }
      Thread.sleep(mScanIntervalSeconds * 1000)
    }
  }

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

    if (mDeployedTemplates.contains(fullyQualifiedName)) {
      // 1) Create a new instance and done.
      createNewInstance(artifact, templateParamValues)
    } else {
      // If not:
      // 1) Download the artifact to a temporary location and moving to the proper location
      val tempArtifactFile = File.createTempFile("artifact", "war")
      artifact.downloadArtifact(tempArtifactFile)
      FileUtils.moveFile(tempArtifactFile, new File(artifact.getFullyQualifiedModelName()))
      // 2) Creating a new Jetty template to map to the war file
      val tempTemplateDir = File.createTempFile("template", "")
      translateFile(JETTY_TEMPLATE_FILE, new File(tempTemplateDir, "template.xml"),
        Map[String, String]())
      val finalTemplateDir = new File(ScoringServer.MODELS_FOLDER, OverlayedAppProvider.TEMPLATES)
      Files.move(tempTemplateDir, finalTemplateDir)

      // 3) Create a new instance.
      createNewInstance(artifact, templateParamValues)

      mDeployedTemplates.add(fullyQualifiedName)
    }
  }

  private def createNewInstance(artifact: ModelArtifact, templateParams: Map[String, String]) = {
    // This will create a new instance by leveraging the template files on the classpath
    // and create the right directory. Maybe first create the directory in a temp location and
    // move to the right place.
    mDeployedLifeCycles.add(artifact)
    val tempInstanceDir = File.createTempFile("instance", System.currentTimeMillis().toString)
    tempInstanceDir.mkdir()

    translateFile(OVERLAY_FILE, new File(tempInstanceDir, "overlay.xml"), templateParams)
    translateFile(WEB_OVERLAY_FILE, new File(tempInstanceDir, "web-overlay.xml"), templateParams)

    val instanceDir = new File(ScoringServer.MODELS_FOLDER, OverlayedAppProvider.INSTANCES)
    Files.move(tempInstanceDir, instanceDir)
  }

  private def translateFile(filePath: String, targetFile: File, fileParams: Map[String, String]) {
    val fileStream = Source.fromInputStream(getClass.getResourceAsStream(filePath))
    val fileWriter = new PrintWriter(targetFile)
    for (line <- fileStream.getLines) {
      val newLine = if (line.matches("%[A-Z_]+%")) {
        fileParams.foldLeft(line) { (result, currentKV) =>
          val (key, value) = currentKV
          line.replace("%" + key + "%", value)
        }
      } else {
        line
      }
      fileWriter.println(newLine)
    }

    fileWriter.close()
  }

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

  private def getAllEnabledLifecycles = {
    mKijiModelRepo.getModelLifecycles(null, 0, Long.MaxValue, 1, true)
  }
}

object ModelRepoScanner {
  def main(args: Array[String]): Unit = {
    val kijiURI = KijiURI.newBuilder("kiji://.env/default").build()
    val modelRepo = KijiModelRepository.open(Kiji.Factory.open(kijiURI))
    val scanner = new ModelRepoScanner(modelRepo, 1)
  }
}