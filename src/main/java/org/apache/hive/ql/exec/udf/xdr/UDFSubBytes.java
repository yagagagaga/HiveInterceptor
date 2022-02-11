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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 截取指定偏移量、指定长度的字节数组
 */
public class UDFSubBytes extends GenericUDF {

	private static final Logger LOG = LoggerFactory.getLogger(
			Thread.currentThread().getStackTrace()[0].getClassName());

	private final ImmutableBytesWritable binaryResult = new ImmutableBytesWritable(true);
	private final Text textResult = new Text();

	@Override
	public String getDisplayString(String[] children) {
		return "subbytes(" + String.join(",", children) + ")";
	}

	@Override
	public ObjectInspector initialize(ObjectInspector[] objectInspectors)
			throws UDFArgumentException {
		if (objectInspectors.length != 3) {
			throw new UDFArgumentLengthException("normalizetimestamp require 3 argument");
		}

		for (int i = 0; i < 3; i++) {
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

		int offset = writableIntObjectInspector.get(deferredObjects[1].get());
		int length = writableIntObjectInspector.get(deferredObjects[2].get());

		if (offset < 0 || length < 0) {
			return null;
		}

		if (deferredObjects[0].get() instanceof ImmutableBytesWritable) {
			return handleBinary(deferredObjects[0], offset, length);
		} else {
			return handleText(deferredObjects[0], offset, length);
		}
	}

	private Object handleText(DeferredObject deferredObject, int offset, int length)
			throws HiveException {
		final String str = javaStringObjectInspector.getPrimitiveJavaObject(deferredObject.get());

		try {
			if (offset + length > str.length()) {
				return null;
			}
			final String substring = str.substring(offset, offset + length);
			textResult.set(substring);
			return textResult;
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
			return null;
		}
	}

	private Object handleBinary(DeferredObject deferredObject, int offset, int length)
			throws HiveException {
		ImmutableBytesWritable buffer = ImmutableBinaryObjectInspector.ImmutableOI.getPrimitiveWritableObject(
				deferredObject.get());

		byte[] bytes = buffer.get();
		int bufferOffset = buffer.getOffset();
		int bufferLength = buffer.getLength();

		if (offset + length > bufferLength) {
			return null;
		} else {
			binaryResult.set(bytes, bufferOffset + offset, length);
			return binaryResult;
		}
	}
}
