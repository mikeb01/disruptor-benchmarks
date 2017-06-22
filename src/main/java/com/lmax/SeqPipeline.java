package com.lmax;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.LifecycleAware;
import com.lmax.disruptor.LiteBlockingWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.Sequencer;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.lmax.disruptor.util.DaemonThreadFactory;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

@State(Scope.Benchmark)
public class SeqPipeline
{
    RingBuffer<PipelinerEvent> rb;
    @Param({"BusySpin", "LiteBlocking"})
    String strategy;

    @Setup
    public void doSetup()
    {
        WaitStrategy waitStrategy;
        switch (strategy)
        {
            case "BusySpin":
                waitStrategy = new BusySpinWaitStrategy();
                break;

            case "LiteBlocking":
                waitStrategy = new LiteBlockingWaitStrategy();
                break;

            default:
                throw new RuntimeException();
        }

        Disruptor<PipelinerEvent> disruptor =
            new Disruptor<>(PipelinerEvent.FACTORY, 1024, DaemonThreadFactory.INSTANCE, ProducerType.SINGLE, waitStrategy);
        disruptor.handleEventsWith(
            new ParallelHandler(0, 3),
            new ParallelHandler(1, 3),
            new ParallelHandler(2, 3))
            .then(new JoiningHandler());
        rb = disruptor.start();
    }

    @Benchmark
    public void measurePipeline()
    {
        long next = rb.next();
        try
        {
            PipelinerEvent pipelinerEvent = rb.get(next);
            pipelinerEvent.input = 2;
        }
        finally
        {
            rb.publish(next);
        }
    }

    private static class ParallelHandler implements EventHandler<PipelinerEvent>
    {
        private final int ordinal;
        private final int totalHandlers;

        ParallelHandler(int ordinal, int totalHandlers)
        {
            this.ordinal = ordinal;
            this.totalHandlers = totalHandlers;
        }

        @Override
        public void onEvent(PipelinerEvent event, long sequence, boolean endOfBatch) throws Exception
        {
            if (sequence % totalHandlers == ordinal)
            {
                event.result = Long.toString(event.input);
            }
        }
    }

    private static class JoiningHandler implements EventHandler<PipelinerEvent>,
        LifecycleAware
    {
        private long cnt = 0;

        @Override
        public void onEvent(PipelinerEvent event, long sequence, boolean endOfBatch) throws Exception
        {
            cnt++;
            event.result = null;
        }

        @Override
        public void onStart()
        {

        }

        @Override
        public void onShutdown()
        {
            System.out.println(cnt);
        }
    }

    private static class PipelinerEvent
    {
        long input;
        Object result;

        private static final EventFactory<PipelinerEvent> FACTORY = new EventFactory<PipelinerEvent>()
        {
            @Override
            public PipelinerEvent newInstance()
            {
                return new PipelinerEvent();
            }
        };

        @Override
        public String toString()
        {
            return "PipelinerEvent{" +
                "input=" + input +
                ", result=" + result +
                '}';
        }
    }
}