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

import java.util.Arrays;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

/**
 * 按字节数分隔的 Hive 序列化器
 */
public class DelimitedByteSerDe extends AbstractSerDe {

	public static final String COLUMN_LENGTHS = "column.lengths";
	private int[] columnLengths;
	private int[] variableLengths;
	/**
	 * 反序列化后，返回的值
	 */
	private ImmutableBytesWritable[] row;
	/**
	 * 反序列化对象检查器
	 */
	private StructObjectInspector soi;

	/**
	 * 根据传入的参数，为Hive表的每个字段创建（反）序列化器
	 */
	@Override
	public void initialize(Configuration configuration, Properties properties) throws SerDeException {
		String columnLengthsStr = properties.getProperty(COLUMN_LENGTHS);
		String[] columnLengthItems = columnLengthsStr.split(",");
		variableLengths = new int[columnLengthItems.length];
		columnLengths = new int[columnLengthItems.length];

		for (int i = 0; i < columnLengthItems.length; i++) {
			String columnLengthItem = columnLengthItems[i];
			Matcher matcher = Pattern.compile("(\\d+)\\+\\[(\\d+)]\\*N")
					.matcher(columnLengthItem);
			if (matcher.find()) {
				columnLengths[i] = Integer.parseInt(matcher.group(1));
				variableLengths[i] = Integer.parseInt(matcher.group(2));
			} else {
				columnLengths[i] = Integer.parseInt(columnLengthItem);
				variableLengths[i] = 0;
			}
		}

		LazySimpleSerDe.SerDeParameters serDeParameters = LazySimpleSerDe.initSerdeParams(
				configuration, properties, this.getClass().getName());
		row = new ImmutableBytesWritable[columnLengths.length];
		for (int i = 0; i < row.length; i++) {
			row[i] = new ImmutableBytesWritable();
		}

		ObjectInspector[] ois = new ObjectInspector[columnLengths.length];
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
	public Object deserialize(Writable writable) throws SerDeException {
		ByteBufWritable input = (ByteBufWritable) writable;
		int pos = 0;
		byte[] bytes = input.getBuffer();

		int i = 0;
		while (i < row.length) {
			pos = input.getReaderIndex();

			int length;
			if (variableLengths[i] == 0) {
				length = columnLengths[i];
			} else {
				if (bytes[pos] > 0) {
					length = bytes[pos] * variableLengths[i] + columnLengths[i];
				} else {
					length = columnLengths[i];
				}
			}

			row[i].set(bytes, pos, length);
			input.addReaderIndex(length);
			i += 1;
		}

		return row;
	}

	@Override
	public ObjectInspector getObjectInspector() throws SerDeException {
		return soi;
	}
}
