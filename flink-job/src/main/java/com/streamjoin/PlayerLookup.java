package com.streamjoin;

import com.streamjoin.model.MatchParticipant;
import com.streamjoin.model.Player;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

public class PlayerLookup extends KeyedCoProcessFunction<Long, MatchParticipant, Player, MatchParticipant> {

    private transient ValueState<Player> playerState;
    private transient Counter playerLookupHits;
    private transient Counter playerLookupMisses;

    @Override
    public void open(Configuration parameters) {
        playerState = getRuntimeContext().getState(
            new ValueStateDescriptor<>("player", Player.class)
        );
        MetricGroup join = getRuntimeContext().getMetricGroup().addGroup("join");
        playerLookupHits = join.counter("player_lookup_hits");
        playerLookupMisses = join.counter("player_lookup_misses");
    }
    
    @Override
    public void processElement1(MatchParticipant participant, Context ctx, Collector<MatchParticipant> out) 
        throws Exception {
        
            Player player = playerState.value();
            if (player == null) {
                playerLookupMisses.inc();
                return;
            }
        playerLookupHits.inc();
        participant.accountName = player.accountName;
        out.collect(participant);
    }
    
    @Override
    public void processElement2(Player player, Context ctx, Collector<MatchParticipant> out) throws Exception {
        playerState.update(player);
        
    }
}