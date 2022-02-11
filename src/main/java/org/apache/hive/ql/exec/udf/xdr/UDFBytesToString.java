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
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hive.serde2.objectinspector.ImmutableBinaryObjectInspector;

@Description(name = "bytestostring", value = "_FUNC_(x) - returns the absolute value of x", extended = "Example:\n"
		+ "  > SELECT _FUNC_(0) FROM src LIMIT 1;\n"
		+ "  0\n"
		+ "  > SELECT _FUNC_(-5) FROM src LIMIT 1;\n"
		+ "  5")
public class UDFBytesToString extends GenericUDF {

	private final ImmutableBytesWritable result = new ImmutableBytesWritable(true);
	private ImmutableBinaryObjectInspector boi;

	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
		if (arguments.length != 1) {
			throw new UDFArgumentLengthException("tbcd require 1 argument");
		}

		if (arguments[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
			throw new UDFArgumentTypeException(0,
					"Only primitive type arguments are accepted but "
							+ arguments[0].getTypeName() + " is passed as first arguments");
		}

		boi = ImmutableBinaryObjectInspector.ImmutableOI;
		return ImmutableBinaryObjectInspector.ImmutableOI;
	}

	@Override
	public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
		ImmutableBytesWritable buffer = boi.getPrimitiveWritableObject(deferredObjects[0].get());
		byte[] bytes = buffer.get();
		int length = buffer.getLength();
		int offset = buffer.getOffset();
		int len = 0;

		while (len < length && bytes[offset + len] != -1 && bytes[offset + len] != 0) {
			len += 1;
		}

		result.set(bytes, offset, len);
		return result;
	}

	@Override
	public String getDisplayString(String[] children) {
		return "bytestostring(" + String.join(",", children) + ")";
	}
}
