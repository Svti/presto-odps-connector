/*
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
package com.aliyun.odps.cupid.presto;

import static java.util.Objects.requireNonNull;

import java.util.List;

import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.NodeProvider;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

public class OdpsSplit implements ConnectorSplit {
	private final String connectorId;
	private final String schemaName;
	private final String tableName;
	private final String inputSplit;
	private final boolean isZeroColumn;
	private final List<HostAddress> addresses;
	private final NodeSelectionStrategy nodeSelectionStrategy;

	@JsonCreator
	public OdpsSplit(@JsonProperty("connectorId") String connectorId, @JsonProperty("schemaName") String schemaName,
			@JsonProperty("tableName") String tableName, @JsonProperty("inputSplit") String inputSplit,
			@JsonProperty("isZeroColumn") boolean isZeroColumn, @JsonProperty("addresses") List<HostAddress> addresses,
			@JsonProperty("nodeSelectionStrategy") NodeSelectionStrategy nodeSelectionStrategy) {
		this.schemaName = requireNonNull(schemaName, "schema name is null");
		this.connectorId = requireNonNull(connectorId, "connector id is null");
		this.tableName = requireNonNull(tableName, "table name is null");
		this.inputSplit = requireNonNull(inputSplit, "inputSplit is null");
		this.isZeroColumn = isZeroColumn;
		this.addresses = ImmutableList.copyOf(requireNonNull(addresses, "addresses is null"));
		this.nodeSelectionStrategy = requireNonNull(nodeSelectionStrategy, "nodeSelectionStrategy is null");
	}

	@JsonProperty
	public String getConnectorId() {
		return connectorId;
	}

	@JsonProperty
	public String getSchemaName() {
		return schemaName;
	}

	@JsonProperty
	public String getTableName() {
		return tableName;
	}

	@JsonProperty
	public String getInputSplit() {
		return inputSplit;
	}

	@JsonProperty
	public boolean getIsZeroColumn() {
		return isZeroColumn;
	}

	@JsonProperty
	public Object getInfo() {
		return this;
	}

	@JsonProperty
	public NodeSelectionStrategy getNodeSelectionStrategy() {
		return nodeSelectionStrategy;
	}

	@JsonProperty
	public List<HostAddress> getAddresses() {
		return addresses;
	}

	@Override
	public List<HostAddress> getPreferredNodes(NodeProvider nodeProvider) {
		return addresses;
	}

}
