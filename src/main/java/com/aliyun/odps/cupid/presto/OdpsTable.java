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

import com.facebook.presto.spi.ColumnMetadata;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.requireNonNull;

public class OdpsTable {
	private final String name;
	private final List<OdpsColumnHandle> dataColumns;
	private final List<OdpsColumnHandle> partitionColumns;
	private final List<ColumnMetadata> columnsMetadata;

	@JsonCreator
	public OdpsTable(@JsonProperty("name") String name, @JsonProperty("dataColumns") List<OdpsColumnHandle> dataColumns,
			@JsonProperty("partitionColumns") List<OdpsColumnHandle> partitionColumns) {
		checkArgument(!isNullOrEmpty(name), "name is null or is empty");
		this.name = requireNonNull(name, "name is null");
		this.dataColumns = ImmutableList.copyOf(requireNonNull(dataColumns, "dataColumns is null"));
		this.partitionColumns = ImmutableList.copyOf(requireNonNull(partitionColumns, "partitionColumns is null"));

		ImmutableList.Builder<ColumnMetadata> columnsMetadata = ImmutableList.builder();
		for (OdpsColumnHandle column : this.dataColumns) {
			ColumnMetadata metadata = ColumnMetadata.builder().setName(column.getName())
					.setComment(Optional.of(column.getColumnMetadata().getComment()))
					.setProperties(column.getColumnMetadata().getProperties())
					.setHidden(column.getColumnMetadata().isHidden())
					.setType(column.getType()).build();
			columnsMetadata.add(metadata);
		}
		for (OdpsColumnHandle column : this.partitionColumns) {
			ColumnMetadata metadata = ColumnMetadata.builder().setName(column.getName())
					.setComment(Optional.of(column.getColumnMetadata().getComment()))
					.setProperties(column.getColumnMetadata().getProperties())
					.setExtraInfo(Optional.of("partition key")).setHidden(false).setType(column.getType()).build();
			columnsMetadata.add(metadata);
		}
		this.columnsMetadata = columnsMetadata.build();
	}

	@JsonProperty
	public String getName() {
		return name;
	}

	@JsonProperty
	public List<OdpsColumnHandle> getDataColumns() {
		return dataColumns;
	}

	@JsonProperty
	public List<OdpsColumnHandle> getPartitionColumns() {
		return partitionColumns;
	}

	public List<ColumnMetadata> getColumnsMetadata() {
		return columnsMetadata;
	}
}
