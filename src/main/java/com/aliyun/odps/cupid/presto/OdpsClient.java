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

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import javax.annotation.Nullable;
import javax.inject.Inject;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.Table;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.cupid.CupidConf;
import com.aliyun.odps.cupid.CupidSession;
import com.aliyun.odps.task.SQLTask;
import com.aliyun.odps.utils.StringUtils;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.NullableValue;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.base.CharMatcher;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import io.airlift.slice.Slices;
import io.airlift.units.Duration;

import static com.google.common.base.Verify.verify;

public class OdpsClient {
	
	private final OdpsConfig odpsConfig;
	protected final boolean caseInsensitiveNameMatching;
	private final Cache<SchemaTableName, Optional<RemoteTableObject>> remoteTables;

	@Inject
	public OdpsClient(OdpsConfig config, JsonCodec<Map<String, List<OdpsTable>>> catalogCodec) {
		requireNonNull(config, "config is null");
		requireNonNull(catalogCodec, "catalogCodec is null");

		odpsConfig = config;

		Map<String, String> hints = new HashMap<>(2);
		hints.put("odps.sql.type.system.odps2", "true");
		hints.put("odps.sql.hive.compatible", "true");
		hints.put("odps.sql.decimal.odps2", "true");
		SQLTask.setDefaultHints(hints);

		CupidConf cupidConf = new CupidConf();
		if (System.getenv("META_LOOKUP_NAME") != null) {
			cupidConf.set("odps.project.name", System.getenv("ODPS_PROJECT_NAME"));
			cupidConf.set("odps.end.point", System.getenv("ODPS_RUNTIME_ENDPOINT"));
		} else {
			cupidConf.set("odps.project.name", config.getProject());
			cupidConf.set("odps.access.id", config.getAccessId());
			cupidConf.set("odps.access.key", config.getAccessKey());
			cupidConf.set("odps.end.point", config.getEndPoint());
			if (!StringUtils.isEmpty(config.getTunnelEndPoint())) {
				cupidConf.set("odps.tunnel.end.point", config.getTunnelEndPoint());
			}
		}
		CupidSession.setConf(cupidConf);

		this.caseInsensitiveNameMatching = config.isCaseInsensitiveNameMatching();
		Duration caseInsensitiveNameMatchingCacheTtl = requireNonNull(config.getCaseInsensitiveNameMatchingCacheTtl(),
				"caseInsensitiveNameMatchingCacheTtl is null");
		CacheBuilder<Object, Object> remoteTableNamesCacheBuilder = CacheBuilder.newBuilder()
				.expireAfterWrite(caseInsensitiveNameMatchingCacheTtl.toMillis(), MILLISECONDS);
		this.remoteTables = remoteTableNamesCacheBuilder.build();

	}

	Optional<RemoteTableObject> toRemoteTable(SchemaTableName schemaTableName) {

		requireNonNull(schemaTableName, "schemaTableName is null");

		verify(CharMatcher.forPredicate(Character::isUpperCase).matchesNoneOf(schemaTableName.getTableName()),
				"Expected table name from internal metadata to be lowercase: %s", schemaTableName);

		if (!caseInsensitiveNameMatching) {
			return Optional.of(RemoteTableObject.of(schemaTableName.getTableName()));
		}

		@Nullable
		Optional<RemoteTableObject> remoteTable = remoteTables.getIfPresent(schemaTableName);
		if (remoteTable != null) {
			return remoteTable;
		}

		// Cache miss, reload the cache
		Map<SchemaTableName, Optional<RemoteTableObject>> mapping = new HashMap<>();
		for (Entry<String, String> entry : getTables().entrySet()) {
			String table = entry.getKey();
			String schema = entry.getValue();
			SchemaTableName cacheKey = new SchemaTableName(schema, table);
			mapping.merge(cacheKey, Optional.of(RemoteTableObject.of(table)), (currentValue, collision) -> currentValue
					.map(current -> current.registerCollision(collision.get().getOnlyRemoteTableName())));
			remoteTables.put(cacheKey, mapping.get(cacheKey));
		}

		// explicitly cache if the requested table doesn't exist
		if (!mapping.containsKey(schemaTableName)) {
			remoteTables.put(schemaTableName, Optional.empty());
		}

		return mapping.containsKey(schemaTableName) ? mapping.get(schemaTableName) : Optional.empty();
	}

	private Odps getOdps() {
		return CupidSession.get().odps();
	}

	public OdpsConfig getOdpsConfig() {
		return odpsConfig;
	}

	public Set<String> getProjectNames() {
		Set<String> projects = new HashSet<>();
		projects.addAll(odpsConfig.getExtraProjectList());
		projects.add(getOdps().getDefaultProject());
		projects.stream().map(e -> e.toLowerCase(ENGLISH)).collect(toImmutableSet());
		return projects;
	}

	public Set<String> getTableNames(String projectName) {
		requireNonNull(projectName, "projectName is null");
		Set<String> tableNames = new HashSet<>();
		getOdps().tables().iterable(projectName).forEach(new Consumer<Table>() {
			@Override
			public void accept(Table table) {
				tableNames.add(table.getName());
			}
		});
		tableNames.stream().map(e -> e.toLowerCase(ENGLISH)).collect(toImmutableSet());
		return tableNames;
	}

	public Map<String, String> getTables() {
		Map<String, String> tables = new HashMap<>();
		for (String project : getProjectNames()) {
			Set<String> sets = getTableNames(project);
			for (String set : sets) {
				tables.put(set, project);
			}
		}
		return tables;
	}

	public OdpsTable getTable(String projectName, String tableName) {
		requireNonNull(projectName, "projectName is null");
		requireNonNull(tableName, "tableName is null");
		try {
			if (!getOdps().tables().exists(projectName, tableName)) {
				return null;
			}
		} catch (OdpsException e) {
			throw new PrestoException(OdpsErrorCode.ODPS_INTERNAL_ERROR, "odps getTable failed!", e);
		}
		return buildOdpsTable(getOdps().tables().get(projectName, tableName));
	}

	private OdpsTable buildOdpsTable(Table table) {
		TableSchema odpsTableSchema = table.getSchema();
		List<OdpsColumnHandle> dataColumns = odpsTableSchema.getColumns().stream()
				.map(e -> OdpsUtils.buildOdpsColumn(e)).collect(Collectors.toList());
		List<OdpsColumnHandle> partitionColumns = odpsTableSchema.getPartitionColumns().stream()
				.map(e -> OdpsUtils.buildOdpsColumn(e)).collect(Collectors.toList());
		return new OdpsTable(table.getName(), dataColumns, partitionColumns);
	}

	public List<OdpsPartition> getOdpsPartitions(String schemaName, String tableName, OdpsTable odpsTable,
			Constraint<ColumnHandle> constraint) {
		Table table = getOdps().tables().get(schemaName, tableName);
		try {
			if (!table.isPartitioned()) {
				return ImmutableList.of(new OdpsPartition(new SchemaTableName(schemaName, tableName)));
			}
		} catch (OdpsException e) {
			throw new RuntimeException(e);
		}

		return table.getPartitions().stream()
				.map(e -> new OdpsPartition(new SchemaTableName(schemaName, tableName), e.getPartitionSpec().toString(),
						getPartitionKVs(e.getPartitionSpec())))
				.map(e -> parseValuesAndFilterPartition(e, odpsTable.getPartitionColumns(), constraint))
				.filter(Optional::isPresent).map(Optional::get).collect(toImmutableList());
	}

	private Map<ColumnHandle, NullableValue> getPartitionKVs(PartitionSpec odpsPartition) {
		Map<ColumnHandle, NullableValue> kvs = new LinkedHashMap<>(2);
		for (String key : odpsPartition.keys()) {
			OdpsColumnHandle columnHandle = new OdpsColumnHandle(key, VarcharType.VARCHAR, "", true);
			kvs.put(columnHandle, new NullableValue(VarcharType.VARCHAR, Slices.utf8Slice(odpsPartition.get(key))));
		}
		return kvs;
	}

	private Optional<OdpsPartition> parseValuesAndFilterPartition(OdpsPartition partition,
			List<OdpsColumnHandle> partitionColumns, Constraint<ColumnHandle> constraint) {
		Map<ColumnHandle, Domain> domains = constraint.getSummary().getDomains().get();
		for (OdpsColumnHandle column : partitionColumns) {
			NullableValue value = partition.getKeys().get(column);
			Domain allowedDomain = domains.get(column);
			if (allowedDomain != null && !allowedDomain.includesNullableValue(value.getValue())) {
				return Optional.empty();
			}
		}

		if (constraint.predicate().isPresent() && !constraint.predicate().get().test(partition.getKeys())) {
			return Optional.empty();
		}

		return Optional.of(partition);
	}

	public TableSchema getTableSchema(String schemaName, String tableName) {
		return getOdps().tables().get(schemaName, tableName).getSchema();
	}

	public void dropTable(String schemaName, String tableName) {
		try {
			getOdps().tables().delete(schemaName, tableName);
		} catch (OdpsException e) {
			throw new RuntimeException(e);
		}
	}

	public void createTable(String projectName, String tableName, List<OdpsColumnHandle> inputColumns,
			List<String> partitionedBy, boolean ignoreExisting) {
		// construct the sqlText
		StringBuilder sqlTextBuilder = new StringBuilder();
		sqlTextBuilder.append("CREATE TABLE ");
		if (ignoreExisting) {
			sqlTextBuilder.append(" IF NOT EXISTS ");
		}
		sqlTextBuilder.append(projectName).append(".`").append(tableName).append("` (");
		int i = 0;
		while (i < inputColumns.size()) {
			OdpsColumnHandle e = inputColumns.get(i);
			sqlTextBuilder.append("`").append(e.getName()).append("` ");
			sqlTextBuilder.append(OdpsUtils.toOdpsType(e.getType(), e.getIsStringType()).getTypeName());
			if (++i < inputColumns.size()) {
				sqlTextBuilder.append(",");
			} else {
				sqlTextBuilder.append(")");
			}
		}

		if (partitionedBy.size() > 0) {
			sqlTextBuilder.append(" PARTITIONED BY ");
			i = 0;
			while (i < partitionedBy.size()) {
				sqlTextBuilder.append("`").append(partitionedBy.get(i)).append("` STRING");
				if (++i < inputColumns.size()) {
					sqlTextBuilder.append(",");
				} else {
					sqlTextBuilder.append(")");
				}
			}
		}
		sqlTextBuilder.append(";");

		executeSql(sqlTextBuilder.toString());
	}

	private void executeSql(String sql) {
		try {
			SQLTask.run(getOdps(), sql).waitForSuccess();
		} catch (OdpsException e) {
			throw new RuntimeException(e);
		}
	}

	static final class RemoteTableObject {
		private final Set<String> remoteTableNames;

		private RemoteTableObject(Set<String> remoteTableNames) {
			this.remoteTableNames = ImmutableSet.copyOf(remoteTableNames);
		}

		public static RemoteTableObject of(String remoteName) {
			return new RemoteTableObject(ImmutableSet.of(remoteName));
		}

		public RemoteTableObject registerCollision(String ambiguousName) {
			return new RemoteTableObject(ImmutableSet.<String>builderWithExpectedSize(remoteTableNames.size() + 1)
					.addAll(remoteTableNames).add(ambiguousName).build());
		}

		public String getAnyRemoteTableName() {
			return Collections.min(remoteTableNames);
		}

		public String getOnlyRemoteTableName() {
			if (!isAmbiguous()) {
				return getOnlyElement(remoteTableNames);
			}

			throw new PrestoException(OdpsErrorCode.ODPS_AMBIGUOUS_OBJECT_NAME,
					"Found ambiguous names in Druid when looking up '" + getAnyRemoteTableName().toLowerCase(ENGLISH)
							+ "': " + remoteTableNames);
		}

		public boolean isAmbiguous() {
			return remoteTableNames.size() > 1;
		}
	}
}
