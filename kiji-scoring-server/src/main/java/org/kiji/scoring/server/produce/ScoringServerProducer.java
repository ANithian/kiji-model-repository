package org.kiji.scoring.server.produce;

import java.io.IOException;
import java.net.URL;
import java.util.Collections;
import java.util.List;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import org.apache.avro.Schema;
import org.apache.commons.io.IOUtils;

import org.kiji.mapreduce.produce.KijiProducer;
import org.kiji.mapreduce.produce.ProducerContext;
import org.kiji.schema.EntityId;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.util.FromJson;

/** KijiProducer implementation which delegates to the ScoringServer to produce a score. */
public class ScoringServerProducer extends KijiProducer {

  public static final String SCORING_SERVER_BASE_URL_CONF_KEY =
      "org.kiji.scoring.server.produce.ScoringServerProducer.scoring-server-base-url";
  public static final String SCORING_SERVER_PRODUCER_MODEL_ID_CONF_KEY =
      "org.kiji.scoring.server.produce.ScoringServerProducer.model-id";
  public static final Gson GSON = new Gson();

  private static final class ScoringServerResponse {
    private long timestamp;
    private String value;
    private String schema;
    private String family;
    private String qualifier;
  }

  private static String getComponentString(
      final EntityId eid
  ) {
    final StringBuilder builder = new StringBuilder();
    builder.append("[");
    final List<String> componentStrings = Lists.newArrayList();
    for (Object component : eid.getComponents()) {
      componentStrings.add(component.toString());
    }
    Collections.reverse(componentStrings);
    builder.append(Joiner.on(',').join(componentStrings));
    builder.append("]");
    return builder.toString();
  }

  /** {@inheritDoc} */
  @Override
  public KijiDataRequest getDataRequest() {
    return KijiDataRequest.builder().build();
  }

  /**
   * {@inheritDoc}
   * <p>Because this producer is intended for use in KijiScoring, the output column is ignored.</p>
   */
  @Override
  public String getOutputColumn() {
    return null;
  }

  /** {@inheritDoc} */
  @Override
  public void produce(final KijiRowData input, final ProducerContext context) throws IOException {
    final String baseURL = getConf().get(SCORING_SERVER_BASE_URL_CONF_KEY);
    final String modelURLExtension = getConf().get(SCORING_SERVER_PRODUCER_MODEL_ID_CONF_KEY);
    final URL scoringEndpoint = new URL(baseURL + modelURLExtension + "?eid=" + getComponentString(input.getEntityId()));

    final String scoreJSON = IOUtils.toString(scoringEndpoint.openStream(), "UTF-8");

    final ScoringServerResponse[] responses = GSON.fromJson(scoreJSON, ScoringServerResponse[].class);

    debug(scoringEndpoint, scoreJSON, responses);

    context.put(FromJson.fromJsonString(responses[0].value, new Schema.Parser().parse(responses[0].schema)));
  }

  private void debug(URL scoringEndpoint, String scoreJSON, ScoringServerResponse[] responses) {
    System.out.println("scoring endpoint is: " + scoringEndpoint.toString());
    System.out.println("scoreJSON is: " + scoreJSON);
    System.out.println("value is: " + responses[0].value);
  }
}
