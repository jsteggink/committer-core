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
import com.norconex.commons.lang.xml.EnhancedXMLStreamWriter;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.reactivestreams.Subscriber;
import reactor.core.publisher.Flux;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.util.List;
import java.util.function.Consumer;

/**
 * Commits documents to the target repository
 * (e.g. search engine) in batch.  That is, multiple documents are expected
 * to be sent in one requests to the target repository.  To achieve this,
 * operations are cached in memory until the commit batch size is reached, then
 * the operations cached so far are sent.
 * <br><br>
 * After being committed, the documents are automatically removed from the 
 * queue.
 * <br><br>
 * If you need to map original document fields with target repository fields,
 * consider using {@link AbstractMappedCommitterSubscriber}.
 *
 * <p>Subclasses implementing {@link IXMLConfigurable} should allow this inner 
 * configuration:</p>
 * <pre>
 *      &lt;commitBatchSize&gt;
 *          (max number of documents to send at once)
 *      &lt;/commitBatchSize&gt;
 *      &lt;queueDir&gt;(path where to queue files)&lt;/queueDir&gt;
 *      &lt;queueName&gt;(name of the queue)&lt;/queueName&gt;
 *      &lt;maxRetries&gt;(max retries upon commit failures)&lt;/maxRetries&gt;
 *      &lt;maxRetryWait&gt;(max delay in milliseconds between retries)&lt;/maxRetryWait&gt;
 * </pre>
 *
 * @author Jeroen Steggink
 * @since 2.1.3
 */
public class BatchReactiveCommitter
        extends AbstractReactiveCommitter {

    private static final Logger LOG = LogManager.getLogger(
            BatchReactiveCommitter.class);

    protected Flux<List<UniversalIdIntQueueMessage>> documentBuffer;

    /** Default commit batch size. */
    public static final int DEFAULT_COMMIT_BATCH_SIZE = 100;

    private int commitBatchSize = DEFAULT_COMMIT_BATCH_SIZE;

    /**
     * Constructor.
     */
    public BatchReactiveCommitter() {
        super();
    }

    /**
     * Initialize the committer.
     */
    public void init() {
        super.init();
        documentBuffer = super.documentProcessor.buffer(commitBatchSize);
    }

    /**
     *  Set the subscriber. There can be only one subscriber.
     * @param subscriber
     */
    public void setSubscriber(AbstractBatchCommitterSubscriber subscriber) {
        subscriber.setQueue(queue);
        documentBuffer.subscribe(subscriber);
    }

    /**
     * Gets the commit batch size.
     * @return commit batch size
     */
    public int getCommitBatchSize() {
        return commitBatchSize;
    }
    /**
     * Sets the commit batch size.
     * @param commitBatchSize commit batch size
     */
    public void setCommitBatchSize(int commitBatchSize) {
        this.commitBatchSize = commitBatchSize;
        if (super.documentProcessor != null) {
            documentBuffer = super.documentProcessor.buffer(commitBatchSize);
        }
    }

    @Override
    protected void saveToXML(XMLStreamWriter writer) throws XMLStreamException {
        writer.writeStartElement("commitBatchSize");
        writer.writeCharacters(String.valueOf(commitBatchSize));
        writer.writeEndElement();
    }

    @Override
    @SuppressWarnings("unchecked")
    /**
     * @inheritDoc
     */
    protected void loadFromXml(XMLConfiguration xml) {
        setCommitBatchSize(xml.getInt("commitBatchSize", DEFAULT_COMMIT_BATCH_SIZE));
        init();
        setSubscriber((AbstractBatchCommitterSubscriber) XMLConfigurationUtil.newInstance(
                xml, "committerSubscriber"));
    }

    public void saveToXML(EnhancedXMLStreamWriter writer) throws XMLStreamException {
        writer.writeElementInteger("commitBatchSize", getCommitBatchSize());
    }

    @Override
    public int hashCode() {
        HashCodeBuilder hashCodeBuilder = new HashCodeBuilder();
        hashCodeBuilder.appendSuper(super.hashCode());
        hashCodeBuilder.append(commitBatchSize);
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
        if (!(obj instanceof BatchReactiveCommitter)) {
            return false;
        }
        BatchReactiveCommitter other = (BatchReactiveCommitter) obj;
        EqualsBuilder equalsBuilder = new EqualsBuilder();
        equalsBuilder.appendSuper(super.equals(other));
        equalsBuilder.append(commitBatchSize, other.commitBatchSize);
        return equalsBuilder.isEquals();
    }

    @Override
    public String toString() {
        ToStringBuilder builder =
                new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE);
        builder.appendSuper(super.toString());
        builder.append("commitBatchSize", commitBatchSize);
        return builder.toString();
    }
}
