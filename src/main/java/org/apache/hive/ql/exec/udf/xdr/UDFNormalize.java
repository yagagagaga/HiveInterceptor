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
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;
import org.apache.hive.serde2.objectinspector.ImmutableBinaryObjectInspector;

/**
 * 将电话号码归一化，本方法兼容{@link ImmutableBytesWritable}和{@link Text}格式的输入
 */
@Description(name = "normalize", value = "_FUNC_(x) - Normalized telephone number to 11 digit format", extended = "Example:\n"
		+ "  > SELECT _FUNC_('+8616914568569') FROM src LIMIT 1;\n"
		+ "  16914568569\n"
		+ "  > SELECT _FUNC_(null) FROM src LIMIT 1;\n"
		+ "  null")
public class UDFNormalize extends GenericUDF {

	private final Text txtResult = new Text();
	private final ImmutableBytesWritable bytesResult = new ImmutableBytesWritable(true);

	@Override
	public String getDisplayString(String[] children) {
		return "normalize(" + String.join(",", children) + ")";
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

	@Override
	public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
		if (deferredObjects[0] == null) {
			return null;
		}

		if (deferredObjects[0].get() instanceof ImmutableBytesWritable) {
			return handleBytes(deferredObjects[0]);
		} else {
			return handleText(deferredObjects[0]);
		}
	}

	private ImmutableBytesWritable handleBytes(DeferredObject deferredObject) throws HiveException {
		ImmutableBytesWritable buffer = ImmutableBinaryObjectInspector.ImmutableOI.getPrimitiveWritableObject(
				deferredObject.get());

		byte[] bytes = buffer.get();
		int length = buffer.getLength();
		int offset = buffer.getOffset();

		if (length == 14 && (bytes[offset] == '0' || bytes[offset] == '+') &&
				bytes[offset + 1] == '8' && bytes[offset + 2] == '6') {
			bytesResult.set(bytes, offset + 3, 11);
		} else if (length == 13 && bytes[offset] == '8' && bytes[offset + 1] == '6') {
			bytesResult.set(bytes, offset + 2, 11);
		} else if (length == 15 && bytes[offset] == '0' && bytes[offset + 1] == '0' &&
				bytes[offset + 2] == '8' && bytes[offset + 3] == '6') {
			bytesResult.set(bytes, offset + 4, 11);
		} else {
			bytesResult.set(bytes, offset, length);
		}

		return bytesResult;
	}

	private Text handleText(DeferredObject deferredObject) throws HiveException {
		Text input = PrimitiveObjectInspectorFactory.javaStringObjectInspector
				.getPrimitiveWritableObject(deferredObject.get());
		if (input == null) {
			return null;
		}

		final String phoneNumber = input.toString();
		final char[] phoneSeq = phoneNumber.toCharArray();
		switch (phoneSeq.length) {
		case 15:
			txtResult.set(phoneNumber.startsWith("0086")
					? phoneNumber.substring(4)
					: phoneNumber);
			break;
		case 14:
			txtResult.set(phoneNumber.startsWith("086") || phoneNumber.startsWith("+86")
					? phoneNumber.substring(3)
					: phoneNumber);
			break;
		case 13:
			txtResult.set(phoneNumber.startsWith("86")
					? phoneNumber.substring(2)
					: phoneNumber);
			break;
		case 12:
			txtResult.set(phoneNumber.startsWith("0")
					? phoneNumber.substring(1)
					: phoneNumber);
			break;
		case 11:
		default:
			txtResult.set(input);
			break;
		}

		return txtResult;
	}
}
