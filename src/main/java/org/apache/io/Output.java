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
package org.apache.io;

public class Output {
	private final byte[] buffer;
	private int count = 0;

	public Output(int size) {
		buffer = new byte[size];
	}

	public void reset() {
		count = 0;
	}

	public void write(byte b) {
		buffer[count++] = b;
	}

	public void write(int i) {
		buffer[count++] = (byte) i;
	}

	public void write(byte[] input, int offset, int length) {
		System.arraycopy(input, offset, buffer, count, length);
		count += length;
	}

	public void write(byte[] input) {
		write(input, 0, input.length);
	}

	public int getLength() {
		return count;
	}

	public byte[] getData() {
		return buffer;
	}

	public byte[] copyData() {
		byte[] data = new byte[count];
		System.arraycopy(buffer, 0, data, 0, count);
		return data;
	}
}
