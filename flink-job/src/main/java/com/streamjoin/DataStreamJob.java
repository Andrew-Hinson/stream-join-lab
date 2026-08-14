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
import com.streamjoin.model.MatchEvent;
import com.streamjoin.model.MatchParticipant;
import com.streamjoin.model.Player;
import com.streamjoin.model.Rank;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;



public class DataStreamJob {

	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.enableCheckpointing(10_000);	

		DataStream<MatchEvent> events = env.fromSource(
			kafkaSource("dbserver1.public.match_events"),
			WatermarkStrategy.noWatermarks(),
			"match_events")
			.map(DebeziumEnvelope::parseMatchEvent)
			.returns(MatchEvent.class)
			.filter(e -> e != null);

		DataStream<MatchParticipant> participants = env.fromSource(
			kafkaSource("dbserver1.public.match_participants"),
			WatermarkStrategy.noWatermarks(),
			"match_participants")
			.map(DebeziumEnvelope::parseMatchParticipant)
			.returns(MatchParticipant.class)
			.filter(p -> p != null);
		
		DataStream<Player> players = env.fromSource(
			kafkaSource("dbserver1.public.players"),
			WatermarkStrategy.noWatermarks(),
			"players")
			.map(DebeziumEnvelope::parsePlayer)
			.returns(Player.class)
			.filter(p -> p != null);

		KeyedStream<MatchParticipant, Long> keyedParticipants = participants.keyBy(p -> p.playerId);
		KeyedStream<Player, Long> keyedPlayers = players.keyBy(p -> p.id);
		KeyedStream<MatchEvent, Long> keyedEvents = events.keyBy(e -> e.id);
		KeyedStream<MatchParticipant, Long> keyedParticipantsByMatch = participants.keyBy(p -> p.matchId);

		keyedParticipants
			.connect(keyedPlayers)
			.process(new PlayerLookup())
			.print("PLAYER_DIMENSION");
		
		keyedParticipantsByMatch
			.connect(keyedEvents)
			.process(new MatchJoin())
			.print("FACTS");

		env.fromSource(
				kafkaSource("dbserver1.public.ranks"),
				WatermarkStrategy.noWatermarks(),
				"ranks")
				.map(DebeziumEnvelope::parseRank)
				.returns(Rank.class)
				.filter(r -> r != null)
				.print("RANK");
		
		env.execute("match-facts-parse-test");
	}

	private static KafkaSource<String> kafkaSource (String topic) {
		String bootstrapServers = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
		return KafkaSource.<String>builder()
			.setBootstrapServers(bootstrapServers)
			.setTopics(topic)
			.setGroupId("flink-stream-join")
			.setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
			.setValueOnlyDeserializer(new SimpleStringSchema())
			.build();

	}
}
