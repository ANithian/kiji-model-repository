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

package org.kiji.modelrepo.tools;

import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import org.kiji.common.flags.Flag;
import org.kiji.modelrepo.KijiModelRepository;
import org.kiji.modelrepo.ModelArtifact;
import org.kiji.schema.KConstants;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiURI;
import org.kiji.schema.tools.BaseTool;

/**
 * Implementation of the model repository list tool.
 * This will scan model repository table and list all requested fields.
 */
public final class ListModelRepoTool extends BaseTool implements KijiModelRepoTool {

  @Flag(name="kiji", usage="Name of the KIJI instance housing the model repository.")
  private String mInstanceName = null;

  private KijiURI mInstanceURI = null;

  @Flag(name="artifact", usage="Regex pattern for artifact name.")
  private String mArtifactPattern = "([0-z]|\\.)+";

  private Matcher mArtifactPatternMatcher = null;

  @Flag(name="group", usage="Regex pattern for group name.")
  private String mGroupPattern = "([0-z]|\\.)+";

  private Matcher mGroupPatternMatcher = null;

  @Flag(name="version", usage="Regex pattern for version.")
  private String mVersionPattern = "([0-z]|\\.)+";

  private Matcher mVersionPatternMatcher = null;

  @Flag(name="location", usage="Print the locations of models.")
  private boolean mLocation = false;

  @Flag(name="production-ready", usage="Print the production readiness of models.")
  private boolean mProductionReady = false;

  @Flag(name="production-ready-only", usage="Print only those models which are production ready.")
  private boolean mProductionReadyOnly = false;

  @Flag(name="message", usage="Print the messages of models.")
  private boolean mMessage = false;

  @Flag(name="definition", usage="Print the model definition.")
  private boolean mDefinition = false;

  @Flag(name="environment", usage="Print the model environment.")
  private boolean mEnvironment = false;

  @Flag(name="max-versions",
      usage="Max number of versions per cell to display ('all' for all versions). Default is 1.")
  private String mMaxVersions = "1";

  private int mMaxVersionsInteger;

  @Override
  protected void validateFlags() throws Exception {
    super.validateFlags();
    // Set up instance.
    if (mInstanceName == null) {
      mInstanceName = KConstants.DEFAULT_INSTANCE_NAME;
    }
    mInstanceURI = KijiURI.newBuilder(mInstanceName).build();

    // Set up max-versions.
    if (mMaxVersions.equals("all")) {
      mMaxVersionsInteger = Integer.MAX_VALUE;
    } else {
      mMaxVersionsInteger = Integer.parseInt(mMaxVersions);
      if (mMaxVersionsInteger < 1) {
        getPrintStream().printf("--max-versions must be positive, got '%s'%n", mMaxVersions);
      }
    }

    // Set up group, artifact, and version pattern matchers
    mArtifactPatternMatcher = Pattern.compile(mArtifactPattern).matcher("");
    mGroupPatternMatcher = Pattern.compile(mGroupPattern).matcher("");
    mVersionPatternMatcher = Pattern.compile(mVersionPattern).matcher("");
  }

  @Override
  public String getName() {
    return MODEL_REPO_TOOL_BASE + getModelRepoToolName();
  }

  @Override
  public String getCategory() {
    return MODEL_REPO_TOOL_CATEGORY;
  }

  @Override
  public String getModelRepoToolName() {
    return "list";
  }

  @Override
  public String getDescription() {
    return "Lists models in model repository.";
  }

  /**
   * Read cmdline options and determine which fields that the user would like to print.
   *
   * @return set of fields to print.
   */
  private Set<String> getFieldsToPrint() {
    final Set<String> fieldsToPrint = Sets.newHashSet();
    if (mLocation) {
      fieldsToPrint.add(ModelArtifact.LOCATION_KEY);
    }
    if (mDefinition) {
      fieldsToPrint.add(ModelArtifact.DEFINITION_KEY);
    }
    if (mEnvironment) {
      fieldsToPrint.add(ModelArtifact.ENVIRONMENT_KEY);
    }
    if (mProductionReady) {
      fieldsToPrint.add(ModelArtifact.PRODUCTION_READY_KEY);
    }
    if (mMessage) {
      fieldsToPrint.add(ModelArtifact.MESSAGES_KEY);
    }
    return fieldsToPrint;
  }

  @Override
  protected int run(List<String> nonFlagArgs) throws Exception {
    // Check that arguments are valid.
    Preconditions.checkArgument(nonFlagArgs.size() == 0,
        "This tool does not accept unnamed arguments: ", nonFlagArgs.toString());
    Preconditions.checkNotNull(mInstanceURI);
    Preconditions.checkNotNull(mArtifactPatternMatcher);
    Preconditions.checkNotNull(mGroupPatternMatcher);
    Preconditions.checkArgument(mMaxVersionsInteger > 0, "Max versions must be positive.");

    // Open model repository table.
    final Kiji kijiInstance = Kiji.Factory.open(mInstanceURI);
    final KijiModelRepository modelRepository = KijiModelRepository.open(kijiInstance);

    // Create list of fields that the user wants to print.
    final Set<String> fieldsToPrint = this.getFieldsToPrint();

    // Query the model repository for a list of entries.
    final Set<ModelArtifact> setOfModels = modelRepository.getModelLifecycles(
        fieldsToPrint,
        mMaxVersionsInteger,
        mProductionReadyOnly);

    // Iterate through list of model row data and appropriately pretty print fields.
    for (final ModelArtifact model : setOfModels) {
      mVersionPatternMatcher.reset(model.getModelVersion().toCanonicalString());
      mGroupPatternMatcher.reset(model.getGroupName());
      mArtifactPatternMatcher.reset(model.getArtifactName());

      // If artifact, group, and version regex match, then print the corresponding row.
      if (mGroupPatternMatcher.matches()
          && mArtifactPatternMatcher.matches()
          && mVersionPatternMatcher.matches()) {
        getPrintStream().println(model.toString());
      }
    }

    modelRepository.close();
    kijiInstance.release();
    return SUCCESS;
  }
}
