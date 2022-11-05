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

import com.aliyun.odps.Column;
import com.aliyun.odps.cupid.table.v1.writer.FileWriter;
import com.aliyun.odps.cupid.table.v1.writer.FileWriterBuilder;
import com.aliyun.odps.cupid.table.v1.writer.WriteSessionInfo;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Binary;
import com.aliyun.odps.data.Varchar;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.SqlDecimal;
import com.facebook.presto.common.type.Type;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import org.apache.commons.codec.binary.Base64;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.common.type.Varchars.isVarcharType;
import static java.lang.Float.intBitsToFloat;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class OdpsPageSink implements ConnectorPageSink {
	private ConnectorSession connectorSession;
	private FileWriter<ArrayRecord> odpsWriter;
	private ArrayRecord record;
	private List<OdpsColumnHandle> columns;

	public OdpsPageSink(ConnectorSession session, OdpsInsertTableHandle handle) {
		connectorSession = session;
		try {
			ByteArrayInputStream bais = new ByteArrayInputStream(Base64.decodeBase64(handle.getWriteSessionInfo()));
			ObjectInputStream in = new ObjectInputStream(bais);
			WriteSessionInfo writeSessionInfo = (WriteSessionInfo) in.readObject();

			// FIXME: should get the right fileindex
			odpsWriter = new FileWriterBuilder(writeSessionInfo, 0).buildRecordWriter();
			record = new ArrayRecord(handle.getOdpsTable().getDataColumns().stream().map(e -> OdpsUtils.toOdpsColumn(e))
					.collect(Collectors.toList()).toArray(new Column[0]));
			columns = handle.getOdpsTable().getDataColumns();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public CompletableFuture<?> appendPage(Page page) {
		for (int position = 0; position < page.getPositionCount(); position++) {
			for (int channel = 0; channel < page.getChannelCount(); channel++) {
				appendColumn(record, page, position, channel, channel);
			}

			try {
				odpsWriter.write(record);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
		return NOT_BLOCKED;
	}

	private void appendColumn(ArrayRecord record, Page page, int position, int channel, int destChannel) {
		Block block = page.getBlock(channel);
		Type type = columns.get(destChannel).getType();
		boolean isStringType = columns.get(destChannel).getIsStringType();
		if (block.isNull(position)) {
			record.set(destChannel, null);
		} else if (TIMESTAMP.equals(type)) {
			record.setTimestamp(destChannel, new Timestamp(type.getLong(block, position) * 1000));
		} else if (REAL.equals(type)) {
			record.setFloat(destChannel, intBitsToFloat((int) type.getLong(block, position)));
		} else if (BIGINT.equals(type)) {
			record.setBigint(destChannel, type.getLong(block, position));
		} else if (INTEGER.equals(type)) {
			record.setInt(destChannel, (int) type.getLong(block, position));
		} else if (SMALLINT.equals(type)) {
			record.setSmallint(destChannel, (short) type.getLong(block, position));
		} else if (TINYINT.equals(type)) {
			record.setTinyint(destChannel, (byte) type.getLong(block, position));
		} else if (BOOLEAN.equals(type)) {
			record.setBoolean(destChannel, type.getBoolean(block, position));
		} else if (DOUBLE.equals(type)) {
			record.setDouble(destChannel, type.getDouble(block, position));
		} else if (isVarcharType(type)) {
			if (isStringType) {
				record.setString(destChannel, type.getSlice(block, position).toStringUtf8());
			} else {
				record.setVarchar(destChannel, new Varchar(type.getSlice(block, position).toStringUtf8()));
			}
		} else if (VARBINARY.equals(type)) {
			record.setBinary(destChannel, new Binary(type.getSlice(block, position).toByteBuffer().array()));
		} else if (type instanceof DecimalType) {
			SqlDecimal sqlDecimal = (SqlDecimal) type.getObjectValue(connectorSession.getSqlFunctionProperties(), block,
					position);
			record.setDecimal(destChannel, sqlDecimal.toBigDecimal());
		} else {
			throw new UnsupportedOperationException("Type is not supported: " + type);
		}
	}

	@Override
	public CompletableFuture<Collection<Slice>> finish() {
		try {
			odpsWriter.close();
			odpsWriter.commit();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return completedFuture(ImmutableList.of());
	}

	@Override
	public void abort() {
		try {
			odpsWriter.close();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

}
