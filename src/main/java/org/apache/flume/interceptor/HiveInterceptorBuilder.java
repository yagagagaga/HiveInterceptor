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

import static org.apache.flume.interceptor.HiveInterceptorConstants.INPUT_COLUMN_DELIMITER;
import static org.apache.flume.interceptor.HiveInterceptorConstants.INPUT_COLUMN_LENGTHS;
import static org.apache.flume.interceptor.HiveInterceptorConstants.INPUT_COLUMN_NUM;
import static org.apache.flume.interceptor.HiveInterceptorConstants.SQL;

import org.apache.commons.lang3.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.Configurables;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hiveprocessor.HiveProcessor;
import org.apache.hiveprocessor.HiveProcessorFactory;

public class HiveInterceptorBuilder implements Interceptor.Builder {

	private static final HiveProcessorFactory FACTORY = new HiveProcessorFactory();

	// -------- Configurable variables --------

	private String sql;
	private String inputColumnLengths;
	private int inputColumnNum;
	private String inputColumnDelimiter;

	// ----------- Other variables ------------

	private boolean binary;
	private FlumeConsumer consumer;

	@Override
	public void configure(Context ctx) {
		Configurables.ensureRequiredNonNull(ctx, SQL);

		this.sql = ctx.getString(SQL);

		this.inputColumnLengths = ctx.getString(INPUT_COLUMN_LENGTHS, "");
		this.inputColumnNum = ctx.getInteger(INPUT_COLUMN_NUM, 0);
		this.inputColumnDelimiter = ctx.getString(INPUT_COLUMN_DELIMITER, "");

		binary = StringUtils.isNotEmpty(inputColumnLengths);
		if (binary) {
			consumer = new FlumeBinaryConsumer();
		} else {
			consumer = new FlumeStringConsumer();
		}
		consumer.configure(ctx);
	}

	@Override
	public Interceptor build() {

		final AbstractSerDe serDe = binary
				? FACTORY.createSerDe(inputColumnLengths)
				: FACTORY.createSerDe(inputColumnNum, inputColumnDelimiter);

		final HiveProcessor<Event> processor = binary
				? FACTORY.createProcessor(inputColumnLengths, sql, consumer)
				: FACTORY.createProcessor(inputColumnNum, inputColumnDelimiter, sql, consumer);

		return new HiveInterceptor(processor, binary, serDe);
	}
}
