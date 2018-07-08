package com.norconex.committer.core;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.BaseSubscriber;

public interface ICommitterSubscriber {

    public PersistentQueue getQueue();
    public void setQueue(PersistentQueue queue);

}
