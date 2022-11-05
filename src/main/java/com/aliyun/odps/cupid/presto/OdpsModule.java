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

import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.FromStringDeserializer;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;

import javax.inject.Inject;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static com.facebook.airlift.json.JsonBinder.jsonBinder;
import static com.facebook.airlift.json.JsonCodec.listJsonCodec;
import static com.facebook.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static java.util.Objects.requireNonNull;

public class OdpsModule implements Module {
	private final String connectorId;
	private final TypeManager typeManager;

	public OdpsModule(String connectorId, TypeManager typeManager) {
		this.connectorId = requireNonNull(connectorId, "connector id is null");
		this.typeManager = requireNonNull(typeManager, "typeManager is null");
	}

	@Override
	public void configure(Binder binder) {
		binder.bind(TypeManager.class).toInstance(typeManager);

		binder.bind(OdpsConnector.class).in(Scopes.SINGLETON);
		binder.bind(OdpsConnectorId.class).toInstance(new OdpsConnectorId(connectorId));
		binder.bind(OdpsMetadata.class).in(Scopes.SINGLETON);
		binder.bind(OdpsClient.class).in(Scopes.SINGLETON);
		binder.bind(OdpsSplitManager.class).in(Scopes.SINGLETON);
		binder.bind(OdpsRecordSetProvider.class).in(Scopes.SINGLETON);
		binder.bind(OdpsPageSinkProvider.class).in(Scopes.SINGLETON);
		configBinder(binder).bindConfig(OdpsConfig.class);

		jsonBinder(binder).addDeserializerBinding(Type.class).to(TypeDeserializer.class);
		jsonCodecBinder(binder).bindMapJsonCodec(String.class, listJsonCodec(OdpsTable.class));
	}

	public static final class TypeDeserializer extends FromStringDeserializer<Type> {
		private static final long serialVersionUID = 8858223395213714281L;
		private final TypeManager typeManager;

		@Inject
		public TypeDeserializer(TypeManager typeManager) {
			super(Type.class);
			this.typeManager = requireNonNull(typeManager, "typeManager is null");
		}

		@Override
		protected Type _deserialize(String value, DeserializationContext context) {
			return typeManager.getType(parseTypeSignature(value));
		}
	}
}
