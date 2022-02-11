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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.ByteStream;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.lazy.LazyInteger;
import org.apache.hadoop.hive.serde2.lazy.LazyLong;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.EventWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

// checkstyle: off
public class FlumeEventSerDe extends AbstractSerDe {
	private static final byte[] emptyBytes = new byte[0];
	List<ObjectInspector> ios;
	private LazySimpleSerDe.SerDeParameters serDeParameters;
	private byte delimiter;
	private PrimitiveObjectInspector.PrimitiveCategory[] types;
	private Object[] row;
	private StructObjectInspector soi;
	private ByteStream.Output serializeStream = new ByteStream.Output();
	private EventWritable result = new EventWritable();

	@Override
	public void initialize(Configuration configuration, Properties properties) throws SerDeException {
		serDeParameters = LazySimpleSerDe.initSerdeParams(configuration, properties, this.getClass().getName());
		List<TypeInfo> typeInfos = serDeParameters.getColumnTypes();
		delimiter = serDeParameters.getSeparators()[0];

		if (typeInfos.size() <= 0) {
			return;
		}

		types = new PrimitiveObjectInspector.PrimitiveCategory[typeInfos.size() - 1];
		row = new Object[typeInfos.size()];
		ios = new ArrayList<>(typeInfos.size());

		for (int i = 0; i < types.length; ++i) {
			PrimitiveTypeInfo type = (PrimitiveTypeInfo) typeInfos.get(i);
			ios.add(TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(typeInfos.get(i)));
			types[i] = type.getPrimitiveCategory();

			switch (type.getPrimitiveCategory()) {
			case STRING:
				row[i] = new Text();
				break;
			case INT:
				row[i] = new IntWritable();
				break;
			case LONG:
				row[i] = new LongWritable();
				break;
			default:
				break;
			}
		}

		ios.add(ObjectInspectorFactory.getStandardMapObjectInspector(
				PrimitiveObjectInspectorFactory.javaStringObjectInspector,
				PrimitiveObjectInspectorFactory.javaStringObjectInspector));

		soi = ObjectInspectorFactory.getStandardStructObjectInspector(
				serDeParameters.getColumnNames(), ios);
	}

	@Override
	public Class<? extends Writable> getSerializedClass() {
		return EventWritable.class;
	}

	@Override
	public Writable serialize(Object o, ObjectInspector objectInspector) throws SerDeException {
		serializeStream.reset();
		StructObjectInspector soi = (StructObjectInspector) objectInspector;
		List<? extends StructField> fields = soi.getAllStructFieldRefs();
		List<Object> list = soi.getStructFieldsDataAsList(o);
		Map<String, String> header = null;

		Iterator objIterator = list.iterator();
		Iterator<? extends StructField> fieldIterator = fields.iterator();

		boolean first = true;

		while (objIterator.hasNext()) {
			Object value = objIterator.next();
			StructField sf = fieldIterator.next();
			ObjectInspector oi = sf.getFieldObjectInspector();

			if (oi.getCategory() == ObjectInspector.Category.MAP) {
				MapObjectInspector moi = (MapObjectInspector) sf.getFieldObjectInspector();
				header = (Map<String, String>) moi.getMap(value);
				continue;
			}

			if (first == false) {
				serializeStream.write(delimiter);
			} else {
				first = false;
			}

			PrimitiveObjectInspector poi = (PrimitiveObjectInspector) oi;
			Object v = poi.getPrimitiveWritableObject(value);

			if (v == null) {
				continue;
			}

			try {
				switch (poi.getTypeInfo().getTypeName()) {
				case "string":
					Text t = (Text) v;
					serializeStream.write(t.getBytes(), 0, t.getLength());
					break;
				case "int":
					LazyInteger.writeUTF8(serializeStream, ((IntWritable) v).get());
					break;
				case "bigint":
					LazyLong.writeUTF8(serializeStream, ((LongWritable) v).get());
					break;
				default:
					throw new RuntimeException("should not here");
				}
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}

		result.setEvent(EventBuilder.withBody(Arrays.copyOf(serializeStream.getData(), serializeStream.getLength()),
				header));
		return result;
	}

	@Override
	public SerDeStats getSerDeStats() {
		return null;
	}

	@Override
	public Object deserialize(Writable writable) throws SerDeException {
		Event e = ((EventWritable) writable).getEvent();
		byte[] buf = e.getBody();
		int start = 0;
		int end = 0;
		int length = buf.length;
		int index = 0;

		while (end <= length) {
			if (end == length || buf[end] == delimiter) {
				if (end == start) {
					switch (types[index]) {
					case STRING:
						((Text) row[index]).set(emptyBytes);
						break;
					case LONG:
					case INT:
						row[index] = null;
						break;
					default:
					}
				} else {
					switch (types[index]) {
					case STRING:
						((Text) row[index]).set(buf, start, end - start);
						break;
					case LONG: {
						long value = LazyLong.parseLong(buf, start, end - start);

						if (row[index] != null) {
							((LongWritable) row[index]).set(value);
						} else {
							row[index] = new LongWritable(value);
						}
					}
						break;
					case INT: {
						int value = LazyInteger.parseInt(buf, start, end - start);

						if (row[index] != null) {
							((IntWritable) row[index]).set(value);
						} else {
							row[index] = new IntWritable(value);
						}
					}
						break;
					default:
					}
				}

				++index;

				if (end != length) {
					start = end + 1;
					end = start;
				} else {
					break;
				}
			} else {
				++end;
			}
		}

		row[index] = e.getHeaders();
		return row;
	}

	@Override
	public ObjectInspector getObjectInspector() throws SerDeException {
		return soi;
	}
}
