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
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hive.serde2.objectinspector.ImmutableBinaryObjectInspector;
import org.apache.io.Output;
import org.apache.util.ByteUtils;

/**
 * 将电话号码归一化，本方法支持{@link ImmutableBytesWritable}格式的输入
 */
@Description(name = "bytestoip", value = "_FUNC_(x) - Transform bytes to IPV4 or TPV6 addr", extended = "Example:\n"
		+ "  > SELECT _FUNC_('0x644be609') FROM src LIMIT 1;\n"
		+ "  100.75.230.9\n"
		+ "  > SELECT _FUNC_('0x2409800b500317050000000000000101') FROM src LIMIT 1;\n"
		+ "  2409:800b:5003:1705:0000:0000:0000:0101\n"
		+ "  > SELECT _FUNC_(null) FROM src LIMIT 1;\n"
		+ "  null")
public class UDFBytesToIP extends GenericUDF {
	private final Output stream = new Output(128);
	private final ImmutableBytesWritable result = new ImmutableBytesWritable(true);
	private ImmutableBinaryObjectInspector binaryOI;

	/**
	 * 设置UDF方法名
	 *
	 * @param children 函数
	 * @return String 函数名称
	 */
	@Override
	public String getDisplayString(String[] children) {
		return "bytestoip(" + String.join(",", children) + ")";
	}

	/**
	 * 函数传参个数和参数类型检查
	 *
	 * @param objectInspectors 参数
	 * @throws UDFArgumentLengthException 参数长度异常
	 * @throws UDFArgumentTypeException   参数类型异常
	 */
	@Override
	public ObjectInspector initialize(ObjectInspector[] objectInspectors)
			throws UDFArgumentLengthException, UDFArgumentTypeException {
		if (objectInspectors.length != 1) {
			throw new UDFArgumentLengthException(
					"bytesToIp() requires 1 argument, got " + objectInspectors.length);
		}
		binaryOI = ImmutableBinaryObjectInspector.ImmutableOI;
		final ObjectInspector inputOI = objectInspectors[0];
		if (inputOI.getCategory() != ObjectInspector.Category.PRIMITIVE) {
			throw new UDFArgumentTypeException(0,
					"Only primitive type arguments are accepted but "
							+ inputOI.getTypeName() + " is passed as first arguments");
		}
		return inputOI;
	}

	/**
	 * 是为解析IP地址,一个十六进制数为0.5个字节 IPV6用十六进制表示&lt;解出来是十六进制&gt;,分8段,中间用:隔开，
	 * 类似于2001:0410:0000:3c01:0000:0000:0000:83bb
	 *
	 * @param deferredObjects 参数
	 * @throws HiveException Hive异常
	 */
	@Override
	public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {

		ImmutableBytesWritable bytesWritable = binaryOI.getPrimitiveWritableObject(deferredObjects[0].get());
		byte[] buffer = bytesWritable.get();

		int length = bytesWritable.getLength();
		int offset = bytesWritable.getOffset();

		stream.reset();
		// 字节个数等于字节长度
		assert (length == 4 || length == 16);
		// case从小到大
		switch (length) {
		case 4: {
			if (ByteUtils.isNotNull(buffer, offset, length)) {
				final String iPv4 = ByteUtils.toIPv4(buffer, offset);
				result.set(iPv4.getBytes());
				return result;
			} else {
				return null;
			}
		}
		case 16:
			if (ByteUtils.isNotNull(buffer, offset, 12)) {
				final String iPv6 = ByteUtils.toIPv6(buffer, offset);
				result.set(iPv6.getBytes());
				return result;
			} else if (ByteUtils.isNotNull(buffer, offset + 12, 4)) {
				final String iPv4 = ByteUtils.toIPv4(buffer, offset + 12);
				result.set(iPv4.getBytes());
				return result;
			} else {
				return null;
			}
		default:
			return null;
		}
	}
}
