package com.streamjoin;

import com.streamjoin.model.MatchParticipant;
import com.streamjoin.model.Player;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

public class PlayerLookup {
    private transient ValueState<Player> playerState;

    @Override
    public void open(Configuration parameters) {
        playerState = getRuntimeContext().getState(
            new ValueStateDescriptor<>("player", Player.class)
        );
    }
    
    @Override
    public void processElement1(MatchParticipant participant, Context ctx, Collector<String> out) {
        throws Exception {
            Player player = playerState.value();
            if (player == null) {
                return;
            }
        }
        out.collect(participant.playerId + " -> " + player.accountName);
    }
    
    @Override
    public void processElement2(Player player, Context ctx, Collector<String> out) {
        throws Exception {
            playerState.update(player);
        }
    }
}