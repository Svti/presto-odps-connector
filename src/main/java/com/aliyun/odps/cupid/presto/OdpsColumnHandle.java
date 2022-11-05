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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.common.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class OdpsColumnHandle implements ColumnHandle {
	private final String name;
	private final String comment;
	private final Type type;
	private final boolean isStringType;

	@JsonCreator
	public OdpsColumnHandle(@JsonProperty("name") String name, @JsonProperty("type") Type type,
			@JsonProperty("name") String comment, @JsonProperty("isStringType") boolean isStringType) {
		this.name = requireNonNull(name, "name is null");
		this.type = requireNonNull(type, "type is null");
		this.comment = comment;
		this.isStringType = isStringType;
	}

	@JsonProperty
	public String getName() {
		return name;
	}

	@JsonProperty
	public Type getType() {
		return type;
	}

	@JsonProperty
	public String getComment() {
		return comment;
	}

	@JsonProperty
	public boolean getIsStringType() {
		return isStringType;
	}

	public ColumnMetadata getColumnMetadata() {
		return new ColumnMetadata(name, type);
	}

	@Override
	public int hashCode() {
		return Objects.hash(name);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if ((obj == null) || (getClass() != obj.getClass())) {
			return false;
		}

		OdpsColumnHandle other = (OdpsColumnHandle) obj;
		return Objects.equals(this.name, other.name) && Objects.equals(this.type, other.type);
	}

	@Override
	public String toString() {
		return toStringHelper(this).add("name", name).add("type", type).toString();
	}
}
