/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hive.serde2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.ByteBufWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hive.serde2.objectinspector.ImmutableBinaryObjectInspector;
import org.apache.util.TlvUtils;

/**
 * 基于 Tlv 编码的 Hive 序列化器
 */
public class TlvByteSerDe extends AbstractSerDe {

	public static final String TLV_COLUMNS = "tlvColumn.lengths";
	private static final int LV_MARK = -1;
	private static final byte[] EMPTY_BYTES = new byte[0];

	private List<Integer> vAndLvColumnLengths;
	private Map<Integer, Integer> tag2IndexMap;
	/**
	 * 反序列化后，返回的值
	 */
	private ImmutableBytesWritable[] rows;
	/**
	 * 反序列化对象检查器
	 */
	private StructObjectInspector soi;

	/**
	 * 根据传入的参数，为Hive表的每个字段创建（反）序列化器
	 */
	@Override
	public void initialize(Configuration configuration, Properties properties) throws SerDeException {
		String columnItermsStr = properties.getProperty(TLV_COLUMNS);
		vAndLvColumnLengths = new ArrayList<>();
		tag2IndexMap = new HashMap<>();

		final String[] columnIterms = columnItermsStr.split(",");
		for (int i = 0; i < columnIterms.length; i++) {
			String item = columnIterms[i];
			if (item.startsWith("v")) {
				final int vLength = Integer.parseInt(item.substring(1));
				vAndLvColumnLengths.add(vLength);
			} else if (item.equals("lv")) {
				vAndLvColumnLengths.add(LV_MARK);
			} else if (item.startsWith("tlv")) {
				final int tag = Integer.parseInt(item.substring(3));
				tag2IndexMap.put(tag, i);
			} else if (item.equals("tail")) {
				// ignored
			} else {
				throw new IllegalArgumentException(""
						+ "输入的参数只能是 v, lv, tlv 中的一个，而且 v 和 tlv 后面必须加一个数字："
						+ "对于 v，后面加的数字表示字节长度，对于 tlv，后面加的数字表示 tag 值。");
			}
		}

		LazySimpleSerDe.SerDeParameters serDeParameters = LazySimpleSerDe.initSerdeParams(
				configuration, properties, this.getClass().getName());

		final int totalLength = columnIterms.length;
		rows = new ImmutableBytesWritable[totalLength];
		for (int i = 0; i < rows.length; i++) {
			rows[i] = new ImmutableBytesWritable(EMPTY_BYTES, 0, 0, false);
		}

		ObjectInspector[] ois = new ObjectInspector[totalLength];
		// 这里所有字段都是可变数组类型
		Arrays.fill(ois, ImmutableBinaryObjectInspector.ImmutableOI);

		soi = ObjectInspectorFactory.getStandardStructObjectInspector(
				serDeParameters.getColumnNames(), Arrays.asList(ois));
	}

	@Override
	public Class<? extends Writable> getSerializedClass() {
		throw new IllegalStateException("Serialization is not supported at present");
	}

	@Override
	public Writable serialize(Object o, ObjectInspector objectInspector) throws SerDeException {
		throw new SerDeException("Serialization is not supported at present");
	}

	@Override
	public SerDeStats getSerDeStats() {
		return new SerDeStats();
	}

	@Override
	public Object deserialize(Writable writable) {
		ByteBufWritable input = (ByteBufWritable) writable;
		byte[] bytes = input.getBuffer();
		resetRows();

		int pos;
		for (int i = 0; i < vAndLvColumnLengths.size(); i++) {

			pos = input.getReaderIndex();
			ImmutableBytesWritable row = rows[i];

			final Integer colLength = vAndLvColumnLengths.get(i);
			if (colLength.equals(LV_MARK)) {
				// 处理 LV
				final int valueOffset = TlvUtils.getLVValueOffset(bytes, pos);
				final int valueLength = TlvUtils.getLVValueLength(bytes, pos);
				row.set(bytes, valueOffset, valueLength);
				input.addReaderIndex(valueOffset - pos + valueLength);
			} else {
				// 处理 V
				row.set(bytes, pos, colLength);
				input.addReaderIndex(colLength);
			}
		}

		// 处理 tlv 字段
		while (!input.isEnd()) {
			pos = input.getReaderIndex();
			final int tag = TlvUtils.getTLVTag(bytes, pos);
			final int valueOffset = TlvUtils.getTLVValueOffset(bytes, pos);
			final int valueLength = TlvUtils.getTLVValueLength(bytes, pos);

			final Integer idx = tag2IndexMap.get(tag);
			if (idx != null) {
				rows[idx].set(bytes, valueOffset, valueLength);
			}

			input.addReaderIndex(valueOffset - pos + valueLength);
		}
		return rows;
	}

	@Override
	public ObjectInspector getObjectInspector() {
		return soi;
	}

	private void resetRows() {
		for (ImmutableBytesWritable row : rows) {
			row.set(EMPTY_BYTES, 0, 0);
		}
	}
}
