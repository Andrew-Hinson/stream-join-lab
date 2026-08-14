package com.streamjoin;
import com.streamjoin.model.MatchEvent;
import com.streamjoin.model.MatchFacts;
import com.streamjoin.model.MatchParticipant;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

public class MatchJoin extends KeyedCoProcessFunction<Long, MatchParticipant, MatchEvent, MatchFacts> {

    private transient ValueState<MatchEvent> eventState;
    private transient ListState<MatchParticipant> participantState;
    private static final long BUFFER_MS = 3_000;
    private transient ValueState<Long> timerState;

    @Override
    public void open(Configuration paremeters) {
        timerState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("timer", Long.class));
        eventState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("event", MatchEvent.class));
        participantState = getRuntimeContext().getListState(
                new ListStateDescriptor<>("participant", MatchParticipant.class));
    }

    @Override
    public void processElement1(MatchParticipant participant, Context ctx, Collector<MatchFacts> out)
            throws Exception {
        participantState.add(participant); 
        armTimer(ctx);
    }

    @Override
    public void processElement2(MatchEvent event, Context ctx, Collector<MatchFacts> out)
            throws Exception { 
        eventState.update(event);
        armTimer(ctx);
    }
    
    private void armTimer(Context ctx) throws Exception {
        Long prev = timerState.value();
        if (prev != null) {
            ctx.timerService().deleteProcessingTimeTimer(prev);
        }
        long t = ctx.timerService().currentProcessingTime() + BUFFER_MS;
        ctx.timerService().registerProcessingTimeTimer(t);
        timerState.update(t);
    }
    
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<MatchFacts> out) throws Exception {
        MatchEvent event = eventState.value();
        java.util.List<MatchParticipant> participants = new java.util.ArrayList<>();
        for (MatchParticipant p : participantState.get()) {
            participants.add(p);
        }
        if (event != null && participants.size() == 12) {
            for (MatchParticipant p : participants) {
                out.collect(toFacts(event, p));
            }
        }
        eventState.clear();
        participantState.clear();
        timerState.clear();
    }
    
    private static MatchFacts toFacts(MatchEvent e, MatchParticipant p) {
        MatchFacts f = new MatchFacts();
    	f.matchId = e.id;
        f.mapName = e.mapName;
        f.matchDurationSeconds = e.matchDurationSeconds;
        f.startedAt = e.startedAt;
        f.endedAt = e.endedAt;
        f.winningTeam = e.winningTeam;
        f.playerId = p.playerId;
        f.team = p.team;
        f.heroPlayed = p.heroPlayed;
        f.kills = p.kills;
        f.deaths = p.deaths;
        f.healing = p.healing;
        f.result = p.result;
        return f;
    }
}