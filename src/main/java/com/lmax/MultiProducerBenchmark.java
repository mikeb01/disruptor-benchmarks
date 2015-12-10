/*
 * Copyright (c) 2014, Oracle America, Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *  * Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *
 *  * Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 *  * Neither the name of Oracle nor the names of its contributors may be used
 *    to endorse or promote products derived from this software without
 *    specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
 * THE POSSIBILITY OF SUCH DAMAGE.
 */

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
//@Threads(1)
public class MultiProducerBenchmark
{
    @Param("1000")
    public int burstSize;

    @Param("3")
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
            return count >= maxMessages;
        }

        public void reset()
        {
            count = 0;
        }
    }

    @Benchmark
    @Group("g")
    @GroupThreads(1)
    public void consumer() throws Exception
    {
        handler.reset();
        poller.poll(handler);
    }
}
