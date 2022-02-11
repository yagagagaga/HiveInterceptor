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

import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaStringObjectInspector;
import static org.apache.hive.serde2.objectinspector.ImmutableBinaryObjectInspector.ImmutableOI;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.Text;
import org.apache.io.Output;

/**
 * 归一化输入的日期： 如果输入的日期格式为<tt>yyyy-MM-dd hh:mm:ss.SSS</tt>，则返回；
 * 如果输入的日期格式为<tt>yyyyMMdd hh:mm:ss:SSS</tt>，则格式化为<tt>yyyy-MM-dd hh:mm:ss.SSS</tt>
 */
@Description(name = "normalizetimestamp", value = "_FUNC_(x) - Format the \"yyyyMMdd hh:mm:ss:SSS\" style timestamp "
		+ "as \"yyyy-MM-dd hh:mm:ss.SSS\" style", extended = "Example:\n"
				+ "  > SELECT _FUNC_('19991213 10:15:03:658') FROM src LIMIT 1;\n"
				+ "  1999-12-13 10:15:03.658\n"
				+ "  > SELECT _FUNC_(null) FROM src LIMIT 1;\n"
				+ "  null")
public class UDFNormalizeTimestamp extends GenericUDF {

	private final Text result = new Text();

	private final Output output = new Output(30);
	private final ImmutableBytesWritable binaryResult = new ImmutableBytesWritable(true);

	@Override
	public String getDisplayString(String[] children) {
		return "normalizetimestamp(" + String.join(",", children) + ")";
	}

	@Override
	public ObjectInspector initialize(ObjectInspector[] objectInspectors)
			throws UDFArgumentException {
		if (objectInspectors.length != 1) {
			throw new UDFArgumentLengthException("normalizetimestamp require 1 argument");
		}

		final ObjectInspector inputOI = objectInspectors[0];
		if (inputOI.getCategory() != ObjectInspector.Category.PRIMITIVE) {
			throw new UDFArgumentTypeException(0,
					"Only primitive type arguments are accepted but "
							+ inputOI.getTypeName() + " is passed as first arguments");
		}

		return inputOI;
	}

	@SuppressWarnings("ManualArrayCopy")
	private void arraycopy(char[] src, int srcPos, char[] dest, int destPos, int len) {
		for (int i = 0; i < len; i++) {
			src[srcPos + i] = dest[destPos + i];
		}
	}

	@Override
	public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
		if (deferredObjects[0] == null || deferredObjects[0].get() == null) {
			return null;
		}

		if (deferredObjects[0].get() instanceof ImmutableBytesWritable) {
			return handleBinary(deferredObjects[0]);
		} else {
			return handleText(deferredObjects[0]);
		}
	}

	private Object handleText(DeferredObject deferredObject) throws HiveException {
		Text input = javaStringObjectInspector.getPrimitiveWritableObject(deferredObject.get());
		if (input == null) {
			return null;
		}

		final char[] chars = input.toString().toCharArray();
		int length = chars.length;

		switch (length) {
		case 23:
			return input;
		case 21:
			char[] ret = new char[23];
			arraycopy(ret, 0, chars, 0, 4);
			ret[4] = '-';
			arraycopy(ret, 5, chars, 4, 2);
			ret[7] = '-';
			arraycopy(ret, 8, chars, 6, 11);
			ret[19] = '.';
			arraycopy(ret, 20, chars, 18, 3);
			result.set(new String(ret));
			return result;
		default:
			return null;
		}
	}

	private Object handleBinary(DeferredObject deferredObject) throws HiveException {
		ImmutableBytesWritable bytes = ImmutableOI.getPrimitiveWritableObject(deferredObject.get());
		byte[] buffer = bytes.get();
		int offset = bytes.getOffset();
		int length = bytes.getLength();

		if (length == 23) {
			return bytes;
		} else if (length == 21) {
			output.reset();
			output.write(buffer[offset]);
			output.write(buffer[offset + 1]);
			output.write(buffer[offset + 2]);
			output.write(buffer[offset + 3]);
			output.write('-');
			output.write(buffer[offset + 4]);
			output.write(buffer[offset + 5]);
			output.write('-');
			output.write(buffer[offset + 6]);
			output.write(buffer[offset + 7]);
			output.write(buffer, offset + 8, 9);
			output.write('.');
			output.write(buffer[offset + 18]);
			output.write(buffer[offset + 19]);
			output.write(buffer[offset + 20]);

			binaryResult.set(output.getData(), 0, 23);
			return binaryResult;
		} else {
			return bytes;
		}
	}
}