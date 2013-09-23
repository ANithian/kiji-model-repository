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

import java.io.BufferedInputStream
import java.io.File
import java.io.FileOutputStream
import java.lang.System
import java.util.jar.JarEntry
import java.util.jar.JarOutputStream
import org.kiji.modelrepo.KijiModelRepository
import org.kiji.schema.Kiji
import org.kiji.schema.layout.KijiTableLayouts
import org.kiji.schema.util.InstanceBuilder
import org.scalatest.BeforeAndAfter
import org.scalatest.Finders
import org.scalatest.FlatSpec
import org.kiji.modelrepo.KijiModelRepository
import org.kiji.schema.Kiji
import org.kiji.schema.layout.KijiTableLayouts
import org.kiji.schema.util.InstanceBuilder
import com.google.common.io.Files
import java.io.FileNotFoundException

class TestScoringServer extends FlatSpec with BeforeAndAfter {

  // NOTE: This is necessary to properly construct the jar. If you change the
  // DummyExtractorScorer.scala file, then please amend this list. Since Scala is weird
  // with generating class files, you may have to inspect the target directory to find
  // classes generated and then add them here. This is used to generate the target artifact
  // that gets used to upload to the model repository.
  val mExtracterScorerClasses = List(
    "/org/kiji/lifecycle/DummyExtractorScorer.class",
    "/org/kiji/lifecycle/DummyExtractorScorer$$anonfun$1.class",
    "/org/kiji/lifecycle/DummyExtractorScorer$$anonfun$2.class",
    "/org/kiji/lifecycle/DummyExtractorScorer$$anonfun$3.class",
    "/org/kiji/lifecycle/DummyExtractorScorer$$anonfun$4.class")

  var mFakeKiji: Kiji = null
  var mTempHome: File = null

  val EMAIL_ADDRESS = "name@company.com"

  before {
    val builder = new InstanceBuilder("default");
    mFakeKiji = builder.build();
    val tableLayout = KijiTableLayouts.getTableLayout("org/kiji/samplelifecycle/user_table.json")
    mFakeKiji.createTable("users", tableLayout)
    val table = mFakeKiji.openTable("users")
    val writer = table.openTableWriter()
    writer.put(table.getEntityId(12345: java.lang.Long), "info", "email", EMAIL_ADDRESS)
    writer.close()
    table.release()

    val tempModelRepoDir = Files.createTempDir()
    tempModelRepoDir.deleteOnExit()
    KijiModelRepository.install(mFakeKiji, tempModelRepoDir.toURI())

    mTempHome = TestUtils.setupServerEnvironment(mFakeKiji.getURI())
  }

  after {
    mFakeKiji.release()
  }

  "ScoringServer" should "deploy and run a single lifecycle" in {
    val jarFile = File.createTempFile("temp_artifact", ".jar")
    val jarOS = new JarOutputStream(new FileOutputStream(jarFile))
    mExtracterScorerClasses.foreach(addToJar(_, jarOS))
    jarOS.close()

    TestUtils.deploySampleLifecycle(mFakeKiji, "0.0.1", jarFile.getAbsolutePath())
    val modelRepo = KijiModelRepository.open(mFakeKiji)

    val server = ScoringServer.getServer(Some(mTempHome.getCanonicalFile()))
    server.start()

    val connector = server.getConnectors()(0)
    // TODO: Eventually remove this sleep but since Jetty right now is set to scan a directory
    // for changes every second, this has to be here until we can control the deployment
    // synchronously (i.e. upon a change in the model repo, complete the deployment to and through
    // registering with Jetty this new application).
    Thread.sleep(5000)

    val response = TestUtils.scoringServerResponse(connector.getLocalPort(),
      "org/kiji/test/sample_lifecycle/0.0.1/?eid=[12345]")
    server.stop()
    assert(Integer.parseInt(response.getValue.toString) == EMAIL_ADDRESS.length())
  }

  "ScoringServer" should "hot undeploy a model lifecycle" in {
    val jarFile = File.createTempFile("temp_artifact", ".jar")
    val jarOS = new JarOutputStream(new FileOutputStream(jarFile))
    mExtracterScorerClasses.foreach(addToJar(_, jarOS))
    jarOS.close()

    TestUtils.deploySampleLifecycle(mFakeKiji, "0.0.1", jarFile.getAbsolutePath())
    val modelRepo = KijiModelRepository.open(mFakeKiji)

    val server = ScoringServer.getServer(Some(mTempHome.getCanonicalFile()))
    server.start()

    val connector = server.getConnectors()(0)
    // TODO: Eventually remove this sleep but since Jetty right now is set to scan a directory
    // for changes every second, this has to be here until we can control the deployment
    // synchronously (i.e. upon a change in the model repo, complete the deployment to and through
    // registering with Jetty this new application).
    Thread.sleep(5000)

    val response = TestUtils.scoringServerResponse(connector.getLocalPort(),
      "org/kiji/test/sample_lifecycle/0.0.1/?eid=[12345]")

    assert(Integer.parseInt(response.getValue().toString) == EMAIL_ADDRESS.length())

    val modelRepoTable = mFakeKiji.openTable("model_repo")
    val writer = modelRepoTable.openTableWriter()
    writer.put(modelRepoTable.getEntityId(TestUtils.groupName + "." + TestUtils.artifactName,
        "0.0.1"), "model", "production_ready", false)
    writer.close()

    // Same comment on the sleep as above.
    Thread.sleep(5000)

    try {
      TestUtils.scoringServerResponse(connector.getLocalPort(),
        "org/kiji/test/sample_lifecycle/0.0.1/?eid=[12345]")
      fail("Scoring server should have thrown a 404 but didn't")
    } catch {
      case ex: FileNotFoundException => {
        assert(true)
      }
    }
    server.stop()
  }

  /**
   * Adds the given classFile to the target JAR output stream. The classFile is assumed to
   * be a resource on the classpath.
   * @param classFile is the class file name to add to the jar file.
   * @param target is the outputstream representing the jar file where the class gets written.
   */
  def addToJar(classFile: String, target: JarOutputStream) {
    val inStream = classOf[System].getResourceAsStream(classFile)
    val entry = new JarEntry(classFile.substring(1))
    target.putNextEntry(entry)
    val in = new BufferedInputStream(inStream);

    val buffer = new Array[Byte](1024);
    var count = in.read(buffer);
    while (count >= 0) {
      target.write(buffer, 0, count);
      count = in.read(buffer);
    }
    target.closeEntry();
    in.close()
  }
}
