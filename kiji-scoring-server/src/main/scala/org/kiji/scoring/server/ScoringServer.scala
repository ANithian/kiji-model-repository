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

import org.eclipse.jetty.deploy.DeploymentManager
import org.eclipse.jetty.overlays.OverlayedAppProvider
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.server.handler.ContextHandlerCollection
import org.eclipse.jetty.server.handler.DefaultHandler
import org.eclipse.jetty.server.handler.HandlerCollection
import org.kiji.modelrepo.KijiModelRepository

import org.kiji.modelrepo.KijiModelRepository
import org.kiji.schema.Kiji
import org.kiji.schema.KijiURI

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

case class ServerConfiguration(port: Int, repo_uri: String, repo_scan_interval: Int)

/**
 * Main entry point for the scoring server. This pulls in and combines various Jetty components
 * to boot a new web server listening for scoring requests.
 */
object ScoringServer {

  val CONF_FILE = "configuration.json"

  val CONF_FOLDER = "conf"
  val MODELS_FOLDER = "models"
  val LOGS_FOLDER = "logs"

  def main(args: Array[String]): Unit = {

    // Check that we started in the right location else bomb out
    if (!startedInProperLocation) {
      System.err.println("Server not started in proper location. Exiting...")
      return
    }

    val config = getConfig
    val kijiURI = KijiURI.newBuilder(config.repo_uri).build()
    val kiji = Kiji.Factory.open(kijiURI)
    val kijiModelRepo = KijiModelRepository.open(kiji)

    // Start the model lifecycle scanner thread that will scan the model repository
    // for changes.
    val lifeCycleScanner = new ModelRepoScanner(kijiModelRepo, config.repo_scan_interval)
    val lifeCycleScannerThread = new Thread(lifeCycleScanner)
    lifeCycleScannerThread.start()

    val server = new Server(config.port)
    val handlers = new HandlerCollection()

    val contextHandler = new ContextHandlerCollection()
    val deploymentManager = new DeploymentManager()
    val overlayedProvider = new OverlayedAppProvider

    overlayedProvider.setScanDir(new File(MODELS_FOLDER))
    // For now scan this directory once per second.
    overlayedProvider.setScanInterval(1)

    deploymentManager.setContexts(contextHandler)
    deploymentManager.addAppProvider(overlayedProvider)

    handlers.addHandler(contextHandler)
    handlers.addHandler(new DefaultHandler())

    server.setHandler(handlers);
    server.addBean(deploymentManager)

    // Gracefully shutdown the deployment thread to let it finish anything that it may be doing
    sys.ShutdownHookThread {
      lifeCycleScanner.shutdown
    }
    server.start();
    server.join();
  }

  /**
   * Checks that the server is started in the right location by ensuring the presence of a few key
   * directories under the conf, models and logs folder.
   *
   * @returns whether or not the key set of folders exist or not.
   */
  def startedInProperLocation: Boolean = {
    val filesToCheck = Array(CONF_FOLDER + "/" + CONF_FILE, MODELS_FOLDER + "/webapps",
      MODELS_FOLDER + "/instances", MODELS_FOLDER + "/templates", LOGS_FOLDER)

    for (file <- filesToCheck) {
      val fileObj = new File(file.toString())
      if (!fileObj.exists()) {
        System.err.println("Error: " + file + " does not exist!")
        return false
      }
    }
    return true
  }

  /**
   * Returns the ServerConfiguration object constructed from conf/configuration.json.
   *
   * @returns the ServerConfiguration object constructed from conf/configuration.json.
   */
  def getConfig: ServerConfiguration = {
    val configMapper = new ObjectMapper
    configMapper.registerModule(DefaultScalaModule)
    configMapper.readValue(new File(CONF_FOLDER, CONF_FILE), classOf[ServerConfiguration])
  }
}
