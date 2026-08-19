package com.streamjoin;


import com.streamjoin.model.MatchFacts;
import com.streamjoin.model.Rank;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;


public class RankAsOf extends KeyedCoProcessFunction<Long, MatchFacts, Rank, MatchFacts>{

    private transient MapState<Long, Rank> ranksByUpdatedAt;
    private transient Counter rankAsOfFound;
    private transient Counter rankAsOfDefaulted;

    @Override
    public void open(Configuration parameters) {
        ranksByUpdatedAt = getRuntimeContext().getMapState(
            new MapStateDescriptor<>("ranks", Long.class, Rank.class));
        MetricGroup join = getRuntimeContext().getMetricGroup().addGroup("join");
        rankAsOfFound = join.counter("rank_as_of_found");
        rankAsOfDefaulted = join.counter("rank_as_of_defaulted");
    }
    
    @Override
    public void processElement1(MatchFacts fact, Context ctx, Collector<MatchFacts> out) throws Exception {
        long asOf = fact.startedAt.toEpochMilli();
        Rank best = null;
        for (Rank r : ranksByUpdatedAt.values()) {
            long t = r.updatedAt.toEpochMilli();
            if (t <= asOf && (best == null || t > best.updatedAt.toEpochMilli())) {
                best = r;
            }
        }
        if (best != null) {
            rankAsOfFound.inc();
            fact.rankTierAtMatchStart = best.rankTier;
            fact.rankDivisionAtMatchStart = best.rankDivision;
            fact.rankPointsAtMatchStart = best.rankPoints;
        } else {
            rankAsOfDefaulted.inc();
            fact.rankTierAtMatchStart = "bronze";
            fact.rankDivisionAtMatchStart = 3;
            fact.rankPointsAtMatchStart = 0;
        }
        out.collect(fact);
    }
    
    @Override
    public void processElement2(Rank rank, Context ctx, Collector<MatchFacts> out) throws Exception {
        ranksByUpdatedAt.put(rank.updatedAt.toEpochMilli(), rank);
    }

    
}
