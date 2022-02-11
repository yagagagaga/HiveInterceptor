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
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.writableIntObjectInspector;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.Text;
import org.apache.hive.serde2.objectinspector.ImmutableBinaryObjectInspector;

/**
 * 从后往前切割掉字节数组某几位
 */
public class UDFCutTail extends GenericUDF {

	private final ImmutableBytesWritable binaryResult = new ImmutableBytesWritable(true);
	private final Text textResult = new Text();

	@Override
	public String getDisplayString(String[] children) {
		return "cuttail(" + String.join(",", children) + ")";
	}

	public ObjectInspector initialize(ObjectInspector[] objectInspectors)
			throws UDFArgumentException {

		if (objectInspectors.length != 2) {
			throw new UDFArgumentLengthException("normalizetimestamp require 2 argument");
		}

		for (int i = 0; i < 2; i++) {
			if (objectInspectors[i].getCategory() != ObjectInspector.Category.PRIMITIVE) {
				throw new UDFArgumentTypeException(i,
						"Only primitive type arguments are accepted but "
								+ objectInspectors[i].getTypeName() + " is passed as " + (i + 1) + " arguments");
			}
		}

		return objectInspectors[0];
	}

	@Override
	public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
		if (deferredObjects[0] == null || deferredObjects[0].get() == null) {
			return null;
		}

		int length = writableIntObjectInspector.get(deferredObjects[1].get());
		if (length < 0) {
			return null;
		}

		if (deferredObjects[0].get() instanceof ImmutableBytesWritable) {
			return handleBinary(deferredObjects[0], length);
		} else {
			return handleText(deferredObjects[0], length);
		}
	}

	private Object handleText(DeferredObject deferredObject, int length) throws HiveException {
		final String data = javaStringObjectInspector.getPrimitiveJavaObject(deferredObject.get());

		if (length > data.length()) {
			return null;
		} else {
			final String substring = data.substring(0, data.length() - length);
			textResult.set(substring);
			return textResult;
		}
	}

	private Object handleBinary(DeferredObject deferredObject, int length) throws HiveException {
		ImmutableBytesWritable buffer = ImmutableBinaryObjectInspector.ImmutableOI.getPrimitiveWritableObject(
				deferredObject.get());

		byte[] bytes = buffer.get();
		int bufferOffset = buffer.getOffset();
		int bufferLength = buffer.getLength();

		if (length >= bufferLength) {
			return null;
		} else {
			binaryResult.set(bytes, bufferOffset, bufferLength - length);
			return binaryResult;
		}
	}
}
