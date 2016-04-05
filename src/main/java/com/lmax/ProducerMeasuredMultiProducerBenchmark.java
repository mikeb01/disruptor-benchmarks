
package com.lmax;

import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.GroupThreads;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.infra.Blackhole;

import com.lmax.disruptor.EventPoller;
import com.lmax.disruptor.EventPoller.PollState;
import com.lmax.disruptor.InsufficientCapacityException;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.YieldingWaitStrategy;

@State(Scope.Group)
@Threads(4)
public class ProducerMeasuredMultiProducerBenchmark
{
    private static final int BUFFER_SIZE = 1024 * 64;
    private final RingBuffer<ValueEvent> ringBuffer =
        RingBuffer.createMultiProducer(ValueEvent.EVENT_FACTORY, BUFFER_SIZE, new YieldingWaitStrategy());

    private final EventPoller<ValueEvent> poller = ringBuffer.newPoller();
    {
        ringBuffer.addGatingSequences(poller.getSequence());
    }

    private long writeValue = 3;

    @AuxCounters
    @State(Scope.Thread)
    public static class PublishCounters {
        public int published;
        public int publishFailed;
        @Setup(Level.Iteration)
        public void clean() {
            published = publishFailed = 0;
        }
    }

    @AuxCounters
    @State(Scope.Thread)
    public static class PollCounters {
        public int polled;
        public int polledNone;
        @Setup(Level.Iteration)
        public void clean() {
            polled = polledNone = 0;
        }
    }


    @Benchmark
    @Group("g")
    @GroupThreads(3)
    public void producer(PublishCounters counters)
    {
        long next;
        try {
            next = ringBuffer.tryNext();
            ringBuffer.get(next).setValue(writeValue);
            ringBuffer.publish(next);
            counters.published++;
        } catch (InsufficientCapacityException e) {
            counters.publishFailed++;
        }

    }
    @Benchmark
    @Group("g")
    @GroupThreads(1)
    public void consumer(Blackhole bh, PollCounters counters) throws Exception
    {
        PollState result = poller.poll((valueEvent, l, b) -> {
            counters.polled++;
            bh.consume(valueEvent);
            return true;
        });
        if(result == PollState.IDLE) {
            counters.polledNone++;
        }
    }
}