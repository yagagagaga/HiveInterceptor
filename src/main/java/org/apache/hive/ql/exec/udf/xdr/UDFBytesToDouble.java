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
package org.apache.hive.ql.exec.udf.xdr;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hive.serde2.objectinspector.ImmutableBinaryObjectInspector;
import org.apache.io.Output;
import org.apache.util.ByteUtils;

public class UDFBytesToDouble extends GenericUDF {
	private final ImmutableBytesWritable result = new ImmutableBytesWritable(true);
	private final Output stream = new Output(64);
	private ImmutableBinaryObjectInspector binaryOI;

	@Override
	public String getDisplayString(String[] children) {
		return "bytesToDouble(" + String.join(",", children) + ")";
	}

	@Override
	public ObjectInspector initialize(ObjectInspector[] objectInspectors)
			throws UDFArgumentLengthException {
		if (objectInspectors.length != 1) {
			throw new UDFArgumentLengthException(
					"bytesToLong() requires 1 argument, got " + objectInspectors.length);
		}

		binaryOI = ImmutableBinaryObjectInspector.ImmutableOI;
		return ImmutableBinaryObjectInspector.ImmutableOI;
	}

	@Override
	public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
		ImmutableBytesWritable bytesWritable = binaryOI.getPrimitiveWritableObject(deferredObjects[0].get());
		byte[] buffer = bytesWritable.get();
		int length = bytesWritable.getLength();
		int offset = bytesWritable.getOffset();

		long value = ByteUtils.toLong(buffer, offset, length);
		if (value != -1) {
			stream.reset();
			ByteUtils.writeDouble(value, stream);
			result.set(stream.getData(), 0, stream.getLength());
			return result;
		} else {
			return null;
		}
	}
}
