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

import java.io.BufferedWriter
import java.io.OutputStreamWriter
import java.util.HashMap

import org.apache.hadoop.conf.Configuration
import org.kiji.express.avro.AvroInputSpec
import org.kiji.express.avro.AvroModelDefinition
import org.kiji.express.avro.AvroModelEnvironment
import org.kiji.express.modeling.config.ModelDefinition
import org.kiji.express.modeling.config.ModelEnvironment
import org.kiji.express.modeling.framework.ScoreProducer
import org.kiji.mapreduce.kvstore.KeyValueStore
import org.kiji.mapreduce.kvstore.impl.KeyValueStoreConfigValidator
import org.kiji.modelrepo.KijiModelRepository
import org.kiji.schema.Kiji
import org.kiji.schema.KijiColumnName
import org.kiji.schema.KijiURI
import org.kiji.schema.tools.ToolUtils
import org.kiji.schema.util.ProtocolVersion
import org.kiji.schema.util.ToJson
import org.kiji.web.KijiWebContext

import org.kiji.express.avro.AvroInputSpec
import org.kiji.express.avro.AvroModelDefinition
import org.kiji.express.avro.AvroModelEnvironment
import org.kiji.express.modeling.config.ModelDefinition
import org.kiji.express.modeling.config.ModelEnvironment
import org.kiji.express.modeling.framework.ScoreProducer
import org.kiji.mapreduce.kvstore.KeyValueStore
import org.kiji.mapreduce.kvstore.impl.KeyValueStoreConfigValidator
import org.kiji.modelrepo.KijiModelRepository
import org.kiji.schema.Kiji
import org.kiji.schema.KijiColumnName
import org.kiji.schema.KijiURI
import org.kiji.schema.tools.ToolUtils
import org.kiji.schema.util.ProtocolVersion
import org.kiji.schema.util.ToJson
import org.kiji.web.KijiWebContext

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.common.base.Preconditions

import javax.servlet.http.HttpServlet
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse

/**
 * Servlet implementation that executes the scoring phase of a model lifecycle deployed
 * from the model repository. The environment and definition configuration is used to construct
 * the appropriate phase implementation objects and the producer wrapping this performs the
 * actual execution.
 */
class GenericScoringServlet extends HttpServlet {

  var mModelEnvironment: ModelEnvironment = null
  var mModelDefinition: ModelDefinition = null
  var mInputKijiURI: String = null

  val MODEL_REPO_URI = "model-repo-uri"
  val MODEL_GROUP = "model-group"
  val MODEL_ARTIFACT = "model-artifact"
  val MODEL_VERSION = "model-version"

  override def init() {
    val uri = getServletConfig().getInitParameter(MODEL_REPO_URI)
    val modelRepoURI = KijiURI.newBuilder(uri.toString()).build()
    val kiji = Kiji.Factory.open(modelRepoURI)

    try {
      // Fetch the model def/env from the repo
      val modelRepo = KijiModelRepository.open(kiji)
      val modelName = getServletConfig().getInitParameter(MODEL_GROUP)
      val modelArtifact = getServletConfig().getInitParameter(MODEL_ARTIFACT)
      val modelVersion = ProtocolVersion.parse(getServletConfig().getInitParameter(MODEL_VERSION))
      val lifeCycleRow = modelRepo.getModelLifeCycle(modelName, modelArtifact, modelVersion)

      val definitionAvro: AvroModelDefinition = lifeCycleRow.getMostRecentValue("model",
        "definition")
      val environmentAvro: AvroModelEnvironment = lifeCycleRow.getMostRecentValue("model",
        "environment")

      // TODO: Is is alright that I am getting the input spec directly through the Avro
      // record as opposed to through the KijiExpress wrapper classes?
      val inputConfig: AvroInputSpec = environmentAvro.getScoreEnvironment()
        .getInputSpec()
        .asInstanceOf[AvroInputSpec]

      mInputKijiURI = inputConfig.getKijiSpecification().getTableUri()

      val defAvro = ToJson.toJsonString(definitionAvro)
      val envAvro = ToJson.toJsonString(environmentAvro)

      mModelEnvironment = ModelEnvironment.fromJson(envAvro)
      mModelDefinition = ModelDefinition.fromJson(defAvro)

      modelRepo.close()
    } finally {
      kiji.release()
    }
  }

  override def doGet(req: HttpServletRequest, resp: HttpServletResponse) {
    val writer = new BufferedWriter(new OutputStreamWriter(resp.getOutputStream()))
    val command = req.getParameter("command")

//    val inputSpec = mModelEnvironment.scoreEnvironment.get.inputConfig.asInstanceOf[KijiInputSpec]

    val kijiURI = KijiURI.newBuilder(mInputKijiURI).build()
    val kiji = Kiji.Factory.open(kijiURI)
    val table = kiji.openTable(kijiURI.getTable())

    val eid = req.getParameter("eid")

    Preconditions.checkNotNull(eid, "Entity ID required!", "")
    if (eid != null) {
      val entityId = ToolUtils.createEntityIdFromUserInputs(eid, table.getLayout());
      val kp = new ScoreProducer()
      val conf = new Configuration()

      conf.set(ScoreProducer.modelDefinitionConfKey, mModelDefinition.toJson)
      conf.set(ScoreProducer.modelEnvironmentConfKey, mModelEnvironment.toJson)
      kp.setConf(conf)

      val boundKVStores = new HashMap[String, KeyValueStore[_, _]]
      KeyValueStoreConfigValidator.get().bindAndValidateRequiredStores(
        kp.getRequiredStores(), boundKVStores)

      val dataReq = kp.getDataRequest()
      val reader = table.openTableReader()
      val rowData = reader.get(entityId, dataReq)
      // TODO: How to know if rowdata is empty to avoid calling score?
      reader.close()
      val ctx = new KijiWebContext(boundKVStores, new KijiColumnName(kp.getOutputColumn))

      kp.setup(ctx)
      kp.produce(rowData, ctx)
      val mapper = new ObjectMapper()
      writer.write(mapper.valueToTree(ctx.getWrittenCells()).toString())
    }

    writer.close()
    table.release()
    kiji.release()
  }
}
