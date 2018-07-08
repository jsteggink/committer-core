package com.norconex.committer.core;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.DeflateSerializer;
import com.esotericsoftware.minlog.Log;
import com.github.ddth.queue.IQueueMessage;
import com.github.ddth.queue.QueueSpec;
import com.github.ddth.queue.impl.RocksDbQueueFactory;
import com.github.ddth.queue.impl.universal.UniversalIdIntQueueMessage;
import com.github.ddth.queue.impl.universal.idint.UniversalRocksDbQueue;
import com.github.ddth.queue.impl.universal.idint.UniversalRocksDbQueueFactory;
import com.norconex.commons.lang.map.ObservableMap;
import com.norconex.commons.lang.map.Properties;
import org.apache.commons.collections4.map.ListOrderedMap;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;

public class PersistentQueue extends AbstractQueue<UniversalIdIntQueueMessage> {

    private static final Logger LOG = LogManager.getLogger(PersistentQueue.class);
    private static UniversalRocksDbQueue queue;

    static private final ThreadLocal<Kryo> kryos = new ThreadLocal<Kryo>() {
        protected Kryo initialValue() {
            Kryo kryo = new Kryo();
            kryo.register(ArrayList.class);
            kryo.register(byte[].class);
            kryo.register(Document.class, new DeflateSerializer(kryo.getDefaultSerializer(Document.class)));
            kryo.register(DocumentAddOperation.class);
            kryo.register(DocumentDeleteOperation.class);
            kryo.register(HashMap.class);
            kryo.register(ListOrderedMap.class);
            kryo.register(ObservableMap.class);
            kryo.register(Properties.class);
            return kryo;
        };
    };

    private String queueName;
    private String queueDir;

    public static class Builder {
        private String queueName;
        private String queueDir;

        public Builder(String queueName, String queueDir) {
            this.queueName = queueName;
            this.queueDir = queueDir;
        }

        public PersistentQueue build() {
            return new PersistentQueue(this);
        }
    }

    private PersistentQueue(Builder builder) {
        this.queueName = builder.queueName;
        this.queueDir = builder.queueDir;
        initRocksDB(queueName, queueDir);
    }

    private void initRocksDB(String queueName, String queueDir) {
        if(LOG.isTraceEnabled()) {
            // Enable Kryo trace logging when trace is enabled
            Log.TRACE();
        }

        QueueSpec queueSpec = new QueueSpec(queueName);
        queueSpec.setField(RocksDbQueueFactory.SPEC_FIELD_STORAGE_DIR, queueDir);

        if (queue == null) {
            queue = new UniversalRocksDbQueueFactory().getQueue(queueSpec);
        }
    }

    /**
     * Register class for serialization.
     * @param type Class
     */
    public void registerClassForSerialization(Class type) {
        kryos.get().register(type);
        LOG.info(String.format("Registered class for Kyro serialization: %s", type.getCanonicalName()));
    }

    @Override
    public int size() {
        return queue.queueSize();
    }

    @Override
    /**
     *
     */
    public boolean isEmpty() {
        return queue.queueSize() == 0;
    }

    @Override
    /**
     * This queue is not to be cleared.
     */
    public void clear() {
        // NO-OP
    }

    @Override
    /**
     *
     */
    public boolean contains(Object o) {
        return false;
    }

    @Override
    /**
     *
     */
    public Iterator iterator() {
        return null;
    }

    @Override
    /**
     *
     */
    public Object[] toArray() {
        return new Object[0];
    }

    @Override
    /**
     *
     */
    public Object[] toArray(Object[] a) {
        return new Object[0];
    }

    @Override
    /**
     *
     */
    public boolean offer(UniversalIdIntQueueMessage message) {
        return queue.queue(message);
    }

    @Override
    /**
     *
     */
    public UniversalIdIntQueueMessage remove() {
        return null;
    }

    @Override
    /**
     *
     */
    public UniversalIdIntQueueMessage poll() {
        return queue.take();
    }

    @Override
    /**
     *
     */
    public UniversalIdIntQueueMessage peek() {
        return null;
    }

    @Override
    /**
     *
     */
    public boolean containsAll(Collection<?> c) {
        return false;
    }

    @Override
    /**
     *
     */
    public boolean removeAll(Collection<?> c) {
        /*for(Object msg : c) {
            ((UniversalIdIntQueueMessage)msg)
        }*/
        return false;
    }

    @Override
    /**
     *
     */
    public boolean retainAll(Collection<?> c) {
        return false;
    }

    /** @param message */
    public static void finish(UniversalIdIntQueueMessage message) {
        queue.finish(message);
    }

    public void finish(UniversalIdIntQueueMessage msg, Finish f) {
        queue.finish(msg);
    }

    public interface Finish {
        void finish(UniversalIdIntQueueMessage message);
    }

    public void destroy() {
        queue.destroy();
    }

    public int getQueueSize() {
        return queue.queueSize();
    }

    public int getEphemeralSize() {
        return queue.ephemeralSize();
    }

    public void requeuAll() {
        for(IQueueMessage msg : queue.getOrphanMessages(0)) {
            queue.requeueSilent(msg);
            queue.finish(msg);
        }
    }

    /**
     *
     * @param messages
     */
    public void requeuAll(Collection<IQueueMessage> messages) {
        for(IQueueMessage msg : queue.getOrphanMessages(0)) {
            queue.requeueSilent(msg);
            queue.finish(msg);
        }
    }

    /**
     * Put a message from the ephemeral queue back to the queue.
     * @param message
     */
    public void requeue(IQueueMessage message) {
        queue.requeueSilent(message);
    }

    public static byte[] serialize(ICommitOperation operation) {

        OutputStream outputStream = new ByteArrayOutputStream();
        Output output = new Output(outputStream);
        kryos.get().writeClassAndObject(output, operation);
        output.close();
        return ((ByteArrayOutputStream) outputStream).toByteArray();
    }

    public static ICommitOperation deserialize(byte[] content) {
        InputStream inputStream = new ByteArrayInputStream(content);
        Input input = new Input(inputStream);
        ICommitOperation operation = (ICommitOperation) kryos.get().readClassAndObject(input);
        return operation;
    }
}
