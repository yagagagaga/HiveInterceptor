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
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hive.serde2.objectinspector.ImmutableBinaryObjectInspector;
import org.apache.io.Output;

public class UDFTbcd extends GenericUDF {
	public static final byte[] DIGITS = new byte[] { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9' };
	private final Output output = new Output(48);
	private final ImmutableBytesWritable result = new ImmutableBytesWritable(true);
	private ImmutableBinaryObjectInspector boi;

	public static void tbcdDes(Output output, byte[] buffer, int offset, int length) {
		for (int i = 0; i < length; ++i) {
			byte b = buffer[offset + i];
			int lower = b & 0x0f;

			if (lower < 10) {
				output.write(DIGITS[lower]);
			} else {
				break;
			}

			int high = (b >>> 4) & 0x0f;

			if (high < 10) {
				output.write(DIGITS[high]);
			} else {
				break;
			}
		}
	}

	@Override
	public ObjectInspector initialize(ObjectInspector[] objectInspectors)
			throws UDFArgumentException {
		if (objectInspectors.length != 1) {
			throw new UDFArgumentLengthException("tbcd require 1 argument");
		}

		if (objectInspectors[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
			throw new UDFArgumentTypeException(0,
					"Only primitive type arguments are accepted but "
							+ objectInspectors[0].getTypeName() + " is passed as first arguments");
		}
		boi = ImmutableBinaryObjectInspector.ImmutableOI;
		return ImmutableBinaryObjectInspector.ImmutableOI;
	}

	@Override
	public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
		ImmutableBytesWritable buffer = boi.getPrimitiveWritableObject(deferredObjects[0].get());
		byte[] bytes = buffer.get();
		int offset = buffer.getOffset();
		int length = buffer.getLength();

		if (buffer.get()[0] == (byte) 0xFF) {
			return null;
		}

		output.reset();
		tbcdDes(output, bytes, offset, length);
		result.set(output.getData(), 0, output.getLength());
		return result;
	}

	@Override
	public String getDisplayString(String[] children) {
		return "tbcd(" + String.join(",", children) + ")";
	}
}
