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
package org.apache.flume.interceptor;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hive.serde2.objectinspector.ImmutableBinaryObjectInspector;
import org.apache.util.ByteUtils;

// CHECKSTYLE:OFF
public class FlumeBinaryConsumer extends FlumeConsumer {

	private void processBinary(ImmutableBytesWritable writable) {
		byte[] buffer = writable.get();
		int offset = writable.getOffset();
		int length0 = writable.getLength();

		switch (length0) {
		case 1: {
			byte b = buffer[offset];
			if (b != -1) {
				int index = b & 0xff;
				byte[] bytes = ByteUtils.decimalChars[index];
				int i = 0;
				int length = bytes.length;

				while (i < length) {
					stream.write(bytes[i]);
					i += 1;
				}
				// stream.write(BytesUtils.decimalChars(index))
			}
		}
			break;
		case 2: {
			byte a = buffer[offset];
			byte b = buffer[offset + 1];

			if (a != -1 || b != -1) {
				// stream.write(BytesUtils.decimalBytes(a & 0xff)(b & 0xff))
				byte[] bytes = ByteUtils.decimalBytes[a & 0xff][b & 0xff];
				int i = 0;
				int length = bytes.length;

				while (i < length) {
					stream.write(bytes[i]);
					i += 1;
				}
			}
		}
			break;
		case 3:
		case 4:
		case 5:
		case 6:
		case 7:
		case 8: {
			if (!ByteUtils.isAllFF(buffer, offset, length0)) {
				long value = ByteUtils.toLong(buffer, offset, length0);
				if (value != 0xffffffffL) {
					ByteUtils.writeLong(stream, value);
				}
			}
		}
			break;
		default: {
			if (ByteUtils.isNotNull(buffer, offset, length0)) {
				if (appendHexPrefix) {
					stream.write('0');
					stream.write('x');
				}

				int index = 0;

				while (index < length0) {
					byte b = buffer[offset + index];
					stream.write(ByteUtils.hexBytes[(b >>> 4) & 0x0f]);
					stream.write(ByteUtils.hexBytes[b & 0x0f]);
					index += 1;
				}
			}
		}
		}
	}

	@Override
	public Event apply(Object[] objects) {
		if (objects == null) {
			return null;
		}
		if (recordNum != 0) {
			stream.write('\n');
		}

		Iterator<?> objIter = Arrays.asList(objects).iterator();

		int i = 0;
		while (objIter.hasNext()) {
			if (i != 0) {
				stream.write(delimiter.getBytes());
			}

			Object obj = objIter.next();

			if (obj != null) {
				ImmutableBytesWritable writable = ImmutableBinaryObjectInspector.ImmutableOI.getPrimitiveWritableObject(obj);

				if (writable.isChars()) {
					stream.write(writable.get(), writable.getOffset(), writable.getLength());
				} else {
					processBinary(writable);
				}
			}
			i += 1;
		}

		recordNum += 1;

		if (recordNum < maxRecordNum) {
			return null;
		} else {
			byte[] buffer = new byte[stream.getLength()];
			System.arraycopy(stream.getData(), 0, buffer, 0, buffer.length);
			Event result = EventBuilder.withBody(buffer, new HashMap<>(extraHeader));
			recordNum = 0;
			stream.reset();

			return result;
		}
	}
}
