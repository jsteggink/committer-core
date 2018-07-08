package com.norconex.committer.core;

import org.junit.Test;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.TopicProcessor;

import java.util.List;

public class TopicProcessorTest {

    @Test
    public void topicProcessorTest() throws InterruptedException {
        TopicProcessor<String> documentProcessor = TopicProcessor.create();
        Flux<List<String>> documentBuffer = documentProcessor.buffer(2);

        documentBuffer.subscribe(new BaseSubscriber<List<String>>() {
            @Override
            protected void hookOnNext(List<String> value) {
                System.out.println(value);
            }

            @Override
            protected void hookOnComplete() {
                System.out.println("DONE");
            }
        });

        documentProcessor.onNext("A");
        documentProcessor.onNext("B");
        documentProcessor.onNext("C");
        documentProcessor.onNext("D");
        documentProcessor.onNext("E");
        documentProcessor.onNext("F");
        documentProcessor.onNext("G");

        documentProcessor.onComplete();
        Thread.sleep(100);
    }

    @Test
    public void test() throws InterruptedException {
        TopicProcessor<String> topicProcessor = TopicProcessor.create();

        topicProcessor.buffer(2)
            .subscribe(new BaseSubscriber<List<String>>() {
                @Override
                protected void hookOnNext(List<String> value) {
                    System.out.println(value);
                }

                @Override
                protected void hookOnComplete() {
                    System.out.println("DONE");
                }
            });

        FluxSink<String> s = topicProcessor.sink();

        s.next("A");
        s.next("B");
        s.next("C");
        s.next("D");
        s.next("E");
        s.next("F");
        s.next("G");
        s.complete();
    }
}
