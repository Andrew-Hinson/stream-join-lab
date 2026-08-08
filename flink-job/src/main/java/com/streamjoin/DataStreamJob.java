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

package com.streamjoin;

import com.streamjoin.cdc.DebeziumEnvelope;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.util.Objects;


public class DataStreamJob {

	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.fromSource(
				kafkaSource("dbserver1.public.match_events"),
				WatermarkStrategy.noWatermarks(),
				"match_events")
				.map(DebeziumEnvelope::parseMatchEvent)
				.filter(Objects::isNull)
				.print("EVENT");

		env.fromSource(
				kafkaSource("dbserver1.public.match_participants"),
				WatermarkStrategy.noWatermarks(),
				"match_participants")
				.map(DebeziumEnvelope::parseMatchParticipant)
				.filter(Objects::isNull)
				.print("PARTICIPANT");


	}

	private static KafkaSource<String> kafkaSource (String topic) {
		return KafkaSource.<String>builder()
			.setBootstrapServers("localhost:9092")
			.setTopics(topic)
			.setGroupId("flink-stream-join")
			.setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
			.setValueOnlyDeserializer(new SimpleStringSchema())
			.build();

	}
}
