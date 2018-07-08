/* Copyright 2010-2015 Norconex Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.norconex.committer.core;

import com.github.ddth.queue.impl.universal.UniversalIdIntQueueMessage;
import com.norconex.commons.lang.config.IXMLConfigurable;
import com.norconex.commons.lang.config.XMLConfigurationUtil;
import com.norconex.commons.lang.map.Properties;
import com.norconex.commons.lang.xml.EnhancedXMLStreamWriter;
import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.commons.configuration.SubnodeConfiguration;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.reactivestreams.Subscriber;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.UnicastProcessor;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

/**
 *
 * <p>Subclasses implementing {@link IXMLConfigurable} should allow this inner configuration:
 *
 * <pre>
 *      &lt;queueName&gt;(name of the queue)&lt;/queueName&gt;
 *      &lt;queueDir&gt;(path of the queue directory)&lt;/queueDir&gt;
 *      &lt;maxRetries&gt;(path of the queue directory)&lt;/maxRetries&gt;
 *      &lt;maxRetryWait&gt;(path of the queue directory)&lt;/maxRetryWait&gt;
 * </pre>
 *
 * @author Jeroen Steggink
 * @since 2.1.3
 */
public abstract class AbstractReactiveCommitter implements ICommitter, IXMLConfigurable {

    private static final Logger LOG = LogManager.getLogger(AbstractReactiveCommitter.class);

    /** Default queue name. **/
    public static final String DEFAULT_QUEUE_NAME = "committer-queue";
    /** Default directory where to queue files. */
    public static final String DEFAULT_QUEUE_DIR = "reactive-committer-queue";

    protected UnicastProcessor<UniversalIdIntQueueMessage> documentProcessor = null;
    protected FluxSink<UniversalIdIntQueueMessage> sink = null;
    protected PersistentQueue queue = null;
    protected List<Class> serializationClasses = new ArrayList<>();

    protected String queueName = DEFAULT_QUEUE_NAME;
    protected String queueDir = DEFAULT_QUEUE_DIR;
    protected int maxRetries;
    protected long maxRetryWait;
    protected QueueDisposable queueDisposable = new QueueDisposable();

    public AbstractReactiveCommitter() {
    }

    /**
     * Initialize the committer.
     */
    public void init() {
        queue = new PersistentQueue.Builder(queueName, queueDir).build();
        // TODO Check if there is another way than this hook.
        // When next operation is dropped, add to queue;
        Hooks.onNextDropped(o -> queue.offer((UniversalIdIntQueueMessage) o));
        LOG.info(String.format("Queue size: %s", queue.getQueueSize()));
        LOG.info(String.format("Ephemeral queue size: %s", queue.getEphemeralSize()));
        if(queue.getEphemeralSize() > 0) {
            queue.requeuAll();
            LOG.info("Requeued all commit operations from ephemeral queue to queue.");
            LOG.info(String.format("Queue size: %s", queue.getQueueSize()));
            LOG.info(String.format("Ephemeral queue size: %s", queue.getEphemeralSize()));
        }
        for(Class clazz : serializationClasses) {
            queue.registerClassForSerialization(clazz);
        }
        documentProcessor = UnicastProcessor.create((Queue) queue, queueDisposable);
        sink = documentProcessor.sink();
    }

    @Override
    /** @inherit */
    public void add(String reference, InputStream content, Properties metadata) {
        UniversalIdIntQueueMessage msg = UniversalIdIntQueueMessage.newInstance();
        byte[] contentByteArray = new byte[0];
        try {
            contentByteArray = IOUtils.toByteArray(content);
        } catch (IOException e) {
            LOG.error(e);
        }
        DocumentAddOperation operation = new DocumentAddOperation(new Document(reference, contentByteArray, metadata));
        msg.setContent(PersistentQueue.serialize(operation));
        sink.next(msg);
        LOG.info(String.format("DocumentAddOperation queued with reference: %s", reference));
    }

    @Override
    /** @inherit */
    public void remove(String reference, Properties metadata) {
        UniversalIdIntQueueMessage msg = UniversalIdIntQueueMessage.newInstance();
        DocumentDeleteOperation operation = new DocumentDeleteOperation(reference);
        msg.setContent(PersistentQueue.serialize(operation));
        sink.next(msg);
        LOG.info(String.format("DocumentDeleteOperation queued with reference: %s", reference));
    }

    /**
     * Only when the last item is added this method should be called. It will send a "complete" to the sink.
     */
    public void commit() {
        LOG.info("Received commit. Sink is completed.");
        sink.complete();
    }

    /**
     *
     * @param queueName
     * @since 2.1.3
     */
    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }

    /**
     *
     * @return
     * @since 2.1.3
     */
    public String getQueueName() {
        return this.queueName;
    }

    /**
     *
     * @param queueDir
     * @since 2.1.3
     */
    public void setQueueDir(String queueDir) {
        this.queueDir = queueDir;
    }

    public String getQueueDir() {
        return this.queueDir;
    }

    /**
     * Gets the maximum number of retries upon batch commit failure.
     * Default is zero (does not retry).
     * @return maximum number of retries
     * @since 2.1.3
     */
    public int getMaxRetries() {
        return maxRetries;
    }

    /**
     * Sets the maximum number of retries upon batch commit failure.
     * @param maxRetries maximum number of retries
     * @since 2.1.3
     */
    public void setMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
    }

    /**
     * Gets the maximum wait time before retrying a failed commit.
     * @return maximum wait time
     * @since 2.1.3
     */

    public long getMaxRetryWait() {
        return maxRetryWait;
    }
    /**
     * Sets the maximum wait time before retrying a failed commit.
     * @param maxRetryWait maximum wait time
     * @since 2.1.3
     */
    public void setMaxRetryWait(long maxRetryWait) {
        this.maxRetryWait = maxRetryWait;
    }

    /**
     *
     * @return
     * @since 2.1.3
     */
    public QueueDisposable getQueueDisposable() {
        return queueDisposable;
    }

    /**
     *
     * @param queueDisposable
     * @since 2.1.3
     */
    public void setQueueDisposable(QueueDisposable queueDisposable) {
        this.queueDisposable = queueDisposable;
    }

    /**
     * Add subscriber which commits every item on the queue one-by-one.
     *
     * @param subscriber
     * @since 2.1.3
     */
    protected void setSubscriber(AbstractCommitterSubscriber subscriber) {
        subscriber.setQueue(queue);
        documentProcessor.subscribe(subscriber);
    }

    /**
     *
     * @return
     */
    public PersistentQueue getQueue() {
        return queue;
    }

    @Override
    public void saveToXML(Writer out) throws IOException {
        try {
            EnhancedXMLStreamWriter writer = new EnhancedXMLStreamWriter(out);
            writer.writeStartElement("committer");
            writer.writeAttribute("class", getClass().getCanonicalName());
            writer.writeAttributeString("xml:space", "preserve");

            if (getQueueDir() != null) {
                writer.writeElementString("queueDir", getQueueDir());
            }

            if (getQueueName() != null) {
                writer.writeElementString("queueName", getQueueName());
            }

            writer.writeElementInteger("maxRetries", getMaxRetries());
            writer.writeElementLong("maxRetryWait", getMaxRetryWait());

            saveToXML(writer);

            writer.writeEndElement();
            writer.flush();
            writer.close();
        } catch (XMLStreamException e) {
            throw new IOException("Cannot save as XML.", e);
        }
    }

    /**
     * Allows subclasses to write their config to xml
     *
     * @param writer the xml being written
     * @throws XMLStreamException problem saving to XML
     */
    protected abstract void saveToXML(XMLStreamWriter writer)
            throws XMLStreamException;

    @Override
    public void loadFromXML(Reader reader) {
        XMLConfiguration xml = XMLConfigurationUtil.newXMLConfiguration(reader);
        // TODO in the future we could have a configurable queue class
        setQueueDir(xml.getString("queueDir", DEFAULT_QUEUE_DIR));
        setQueueName(xml.getString("queueName", DEFAULT_QUEUE_NAME));
        setMaxRetries(xml.getInt("maxRetries", 0));
        setMaxRetryWait(XMLConfigurationUtil.getDuration(xml, "maxRetryWait", 0));
        SubnodeConfiguration serializationClasses =  xml.configurationAt("serializationClasses");
        for(String clazz : serializationClasses.getStringArray("class")) {
            try {
                this.serializationClasses.add(Class.forName(clazz));
            } catch (Exception e) {
                LOG.error(String.format("Class %s not found for serialization.", clazz));
            }
        }
        loadFromXml(xml);
    }

    /**
     * Allows subclasses to load their config from xml. This is where you should call init().
     *
     * @param xml XML configuration
     */
    protected void loadFromXml(XMLConfiguration xml) {
        init();
        setSubscriber(XMLConfigurationUtil.newInstance(
                xml, "committerSubscriber"));
    };

    @Override
    public int hashCode() {
        HashCodeBuilder hashCodeBuilder = new HashCodeBuilder();
        return hashCodeBuilder.toHashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof AbstractReactiveCommitter)) {
            return false;
        }
        AbstractReactiveCommitter other = (AbstractReactiveCommitter) obj;
        EqualsBuilder equalsBuilder = new EqualsBuilder();
        return equalsBuilder.isEquals();
    }

    @Override
    public String toString() {
        ToStringBuilder builder = new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE);
        return builder.toString();
    }
}
