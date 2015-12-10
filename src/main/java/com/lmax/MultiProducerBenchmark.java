
package com.lmax;

import com.lmax.disruptor.EventPoller;
import com.lmax.disruptor.InsufficientCapacityException;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.YieldingWaitStrategy;
import org.openjdk.jmh.annotations.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

@State(Scope.Group)
@Threads(1)
public class MultiProducerBenchmark
{
    @Param("1000")
    public int burstSize;

    @Param("1")
    public int producerCount;

    private static final int BUFFER_SIZE = 1024 * 64;
    private final RingBuffer<ValueEvent> ringBuffer =
        RingBuffer.createMultiProducer(ValueEvent.EVENT_FACTORY, BUFFER_SIZE, new YieldingWaitStrategy());

    private final EventPoller<ValueEvent> poller = ringBuffer.newPoller();
    {
        ringBuffer.addGatingSequences(poller.getSequence());
    }

    private List<Thread> producerThreads = new ArrayList<Thread>();
    private AtomicBoolean running;
    private final ValueHandler handler = new ValueHandler();

    @Setup(Level.Trial)
    public void setup() throws Exception
    {
        running = new AtomicBoolean(true);

        for (int i = 0; i < producerCount; i++)
        {
            final Thread pThread = new Thread(new Producer(running, ringBuffer, burstSize));

            pThread.setName("publisher");

            pThread.start();
            producerThreads.add(pThread);
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() throws InterruptedException
    {
        running.set(false);
        for (Thread publisherThread : producerThreads)
        {
            publisherThread.join();
        }
    }

    @AuxCounters
    @State(Scope.Thread)
    public static class ConsumerCounters
    {
        public long messages;

        @Setup(Level.Iteration)
        public void resetCounters()
        {
            messages = 0;
        }

        public void add(int count)
        {
            messages += count;
        }
    }

    private long writeValue = 3;

    private static class Producer implements Runnable
    {
        private final AtomicBoolean running;
        private final RingBuffer<ValueEvent> ringBuffer;
        private final int burstSize;
        private long writeValue = 3;

        public Producer(AtomicBoolean running, RingBuffer<ValueEvent> ringBuffer, final int burstSize)
        {
            this.running = running;
            this.ringBuffer = ringBuffer;
            this.burstSize = burstSize;
        }

        @Override
        public void run()
        {
            final int burstSize = this.burstSize;
            final AtomicBoolean running = this.running;

            while (running.get())
            {
                for (int i = 0; i < burstSize; i++)
                {
                    retry:
                    try
                    {
                        final long l = ringBuffer.tryNext();
                        ringBuffer.get(l).setValue(writeValue);
                        ringBuffer.publish(l);
                    }
                    catch (InsufficientCapacityException e)
                    {
                        if (!running.get())
                        {
                            return;
                        }

                        break retry;
                    }
                }
            }
        }
    }

//    @Benchmark
//    @Group("g")
//    @GroupThreads(3)
//    public void producer()
//    {
//        long next = ringBuffer.next();
//        ringBuffer.get(next).setValue(writeValue);
//        ringBuffer.publish(next);
//    }

    private static class ValueHandler implements EventPoller.Handler<ValueEvent>
    {
        private final int maxMessages = 64;
        private int count = 0;

        @Override
        public boolean onEvent(final ValueEvent valueEvent, final long l, final boolean b) throws Exception
        {
            count++;
            return count < maxMessages;
        }

        public int reset()
        {
            int c = count;
            count = 0;
            return c;
        }
    }

    @Benchmark
    @Group("g")
    @GroupThreads(1)
    public void consumer(ConsumerCounters counters) throws Exception
    {
        poller.poll(handler);
        counters.add(handler.reset());
    }
}
