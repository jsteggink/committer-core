package com.norconex.committer.core;

import org.junit.Test;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.publisher.UnicastProcessor;
import reactor.util.context.Context;

import java.util.List;

import static org.junit.Assert.assertThat;

public class UnicastProcessorTest {

    @Test
    public void test() throws InterruptedException {
        UnicastProcessor<String> unicastProcessor = UnicastProcessor.create();

        unicastProcessor.buffer(2).subscriberContext(Context.of("test", "blaat"))
                .subscribe(new BaseSubscriber<List<String>>() {
                    @Override
                    protected void hookOnNext(List<String> value) {
                        Mono<Context> c = Mono.subscriberContext();
                        System.out.println(value);


                    }

                    @Override
                    protected void hookOnComplete() {
                        System.out.println("DONE");
                    }
                });

        FluxSink<String> s = unicastProcessor.sink();

        s.next("A");
        Thread.sleep(100);
        s.next("B");
        Thread.sleep(100);
        s.next("C");
        Thread.sleep(100);
        s.next("D");
        Thread.sleep(100);
        s.next("E");
        Thread.sleep(100);
        s.next("F");
        Thread.sleep(100);
        s.next("G");
        Thread.sleep(100);
        s.complete();
    }

    @Test
    public void contextTest() {
        UnicastProcessor<Integer> p = UnicastProcessor.create();
        p.subscriberContext(ctx -> ctx.put("foo", "bar")).subscribe();

        System.out.println(p.sink().currentContext().get("foo").toString());
    }
}
