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
package org.apache.flume.interceptor;

public final class HiveInterceptorConstants {

	public static final String SQL = "sql";
	public static final String INPUT_COLUMN_LENGTHS = "input-column-lengths";
	public static final String INPUT_COLUMN_NUM = "input-column-num";
	public static final String INPUT_COLUMN_DELIMITER = "input-column-delimiter";

	public static final String DELIMITER = "delimiter";
	public static final String APPEND_HEX_PREFIX = "append-hex-prefix";
	public static final String MAX_RECORD_NUM = "max-record-num";
	public static final String BUFFER_SIZE = "buffer-size";
	public static final String EXTRA_HEADER = "extra-header";

	private HiveInterceptorConstants() {
		throw new IllegalStateException("Tool class cannot be instantiated!");
	}
}
