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

public class Input {
	private final byte[] buffer;
	private final int length;
	private final int offset;
	private int pos;

	public Input(Input input) {
		this(input.buffer, input.offset, input.length);
	}

	public Input(byte[] buffer) {
		this(buffer, 0, buffer.length);
	}

	public Input(byte[] buffer, int offset, int length) {
		this.buffer = buffer;
		this.offset = offset;
		this.length = length;
		this.pos = offset;
	}

	public void addReaderIndex(int num) {
		pos += num;
	}

	public int getReaderIndex() {
		if (isEnd()) {
			throw new IndexOutOfBoundsException(
					String.format("Your's length is %d, but your's pos is %d",
							length(), pos - offset));
		}
		return pos;
	}

	public int length() {
		return length;
	}

	public byte[] getBuffer() {
		return buffer;
	}

	public void reset() {
		pos = offset;
	}

	public boolean isEnd() {
		return pos >= length();
	}
}
