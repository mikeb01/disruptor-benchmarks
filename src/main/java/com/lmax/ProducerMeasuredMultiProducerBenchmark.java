
package com.lmax;

import com.lmax.disruptor.*;
import org.openjdk.jmh.annotations.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

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

    @Benchmark
    @Group("g")
    @GroupThreads(3)
    public void producer()
    {
        long next = ringBuffer.next();
        ringBuffer.get(next).setValue(writeValue);
        ringBuffer.publish(next);
    }

    @Benchmark
    @Group("g")
    @GroupThreads(1)
    public void consumer() throws Exception
    {
        poller.poll((valueEvent, l, b) -> true);
    }
}