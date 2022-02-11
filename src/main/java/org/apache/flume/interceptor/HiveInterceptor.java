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

import java.util.List;
import java.util.stream.Collectors;

import org.apache.flume.Event;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.io.ByteBufWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hiveprocessor.HiveProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveInterceptor implements Interceptor {

	private static final Logger LOG = LoggerFactory.getLogger(
			Thread.currentThread().getStackTrace()[0].getClassName());

	private final HiveProcessor<Event> processor;
	private final boolean isBinary;
	private final AbstractSerDe serDe;

	public HiveInterceptor(HiveProcessor<Event> processor, boolean binary, AbstractSerDe serDe) {
		this.processor = processor;
		this.isBinary = binary;
		this.serDe = serDe;
	}

	@Override
	public void initialize() {
		// do nothing
	}

	@Override
	public Event intercept(Event event) {
		Writable w = isBinary
				? new ByteBufWritable(event.getBody())
				: new Text(event.getBody());

		try {
			final Object deserializedRow = serDe.deserialize(w);
			final Event event1 = processor.process(deserializedRow);
			if (event1 != null) {
				event1.getHeaders().putAll(event.getHeaders());
			}
			return event1;
		} catch (SerDeException e) {
			LOG.error("intercept failed!, beacause " + e.getMessage(), e);
			return null;
		}
	}

	@Override
	public List<Event> intercept(List<Event> list) {
		return list.stream()
				.map(this::intercept)
				.collect(Collectors.toList());
	}

	@Override
	public void close() {
		// do nothing
	}
}
