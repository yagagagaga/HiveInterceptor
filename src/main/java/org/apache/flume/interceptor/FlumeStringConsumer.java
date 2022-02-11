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

import java.util.HashMap;
import java.util.StringJoiner;

import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;

public class FlumeStringConsumer extends FlumeConsumer {

	@Override
	public Event apply(Object[] objects) {

		if (objects == null) {
			return null;
		}
		
		if (recordNum != 0) {
			stream.write('\n');
		}
		
		StringJoiner joiner = new StringJoiner(delimiter);
		for (Object str : objects) {

			joiner.add(str == null ? "" : str.toString());
		}
		stream.write(joiner.toString().getBytes());
		recordNum += 1;

		if (recordNum < maxRecordNum) {
			return null;
		} else {
			byte[] buffer = new byte[stream.getLength()];
			System.arraycopy(stream.getData(), 0, buffer, 0, buffer.length);
			Event result = EventBuilder.withBody(buffer, new HashMap<>(extraHeader));
			recordNum = 0;
			stream.reset();

			return result;
		}
	}
}
