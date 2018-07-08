package com.norconex.committer.core;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class PersistentQueueTest {

    PersistentQueue persistentQueue;

    @Before
    public void setUp() throws Exception {
        persistentQueue = new PersistentQueue.Builder("test", "queue").build();
    }

    @Test
    public void testAdding()
    {
        //persistentQueue.add(new DocumentAddOperation(new Document("1")));

        //assertEquals(1, persistentQueue.size());
    }

    @Test
    public void testAddingRemoving()
    {
        //persistentQueue.add(new DocumentAddOperation(new Document("1")));

        //assertEquals(0, persistentQueue.size());
    }

    @Test
    public void testPoll() {
        //persistentQueue.add(new DocumentAddOperation(new Document("1")));
        //persistentQueue.poll();


    }
}