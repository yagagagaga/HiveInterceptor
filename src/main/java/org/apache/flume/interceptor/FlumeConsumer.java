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

import static org.apache.flume.interceptor.HiveInterceptorConstants.APPEND_HEX_PREFIX;
import static org.apache.flume.interceptor.HiveInterceptorConstants.BUFFER_SIZE;
import static org.apache.flume.interceptor.HiveInterceptorConstants.DELIMITER;
import static org.apache.flume.interceptor.HiveInterceptorConstants.EXTRA_HEADER;
import static org.apache.flume.interceptor.HiveInterceptorConstants.MAX_RECORD_NUM;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.Configurable;
import org.apache.io.Output;

public abstract class FlumeConsumer implements Function<Object[], Event>, Configurable {
	protected Map<String, String> extraHeader;
	protected boolean appendHexPrefix;
	protected int recordNum = 0;
	protected int maxRecordNum;
	protected String delimiter;
	protected Output stream;

	@Override
	public void configure(Context context) {
		delimiter = context.getString(DELIMITER, "|");
		appendHexPrefix = context.getBoolean(APPEND_HEX_PREFIX, true);
		maxRecordNum = context.getInteger(MAX_RECORD_NUM, 1);

		int buffersize = context.getInteger(BUFFER_SIZE, 1024 * 1024);

		final String header = context.getString(EXTRA_HEADER, "");
		extraHeader = Collections.unmodifiableMap(
				Arrays.stream(StringUtils.splitPreserveAllTokens(header, ","))
						.map(kv -> StringUtils.splitPreserveAllTokens(kv, "=", 2))
						.collect(Collectors.toMap(kv -> kv[0], kv -> kv[1])));

		stream = new Output(buffersize);
	}
}
