package com.norconex.committer.core;

import com.github.ddth.queue.impl.universal.UniversalIdIntQueueMessage;
import com.norconex.commons.lang.config.IXMLConfigurable;
import com.norconex.commons.lang.config.XMLConfigurationUtil;
import com.norconex.commons.lang.xml.EnhancedXMLStreamWriter;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.reactivestreams.Subscription;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.SignalType;
import reactor.util.context.Context;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;

/**
 * This is a basic subscriber which can be used for single message processing.
 */
public abstract class AbstractCommitterSubscriber extends BaseSubscriber<UniversalIdIntQueueMessage>
        implements ICommitterSubscriber, IXMLConfigurable {

    private static final Logger LOG = LogManager.getLogger(AbstractCommitterSubscriber.class);
    private PersistentQueue queue;
    protected Subscription subscription;
    Context context = Context.empty();

    public AbstractCommitterSubscriber() { }

    /**
     * Get persistent queue.
     *
     * @return
     */
    public PersistentQueue getQueue() {
        return queue;
    }

    public void setQueue(PersistentQueue queue) {
        this.queue = queue;
    }

    @Override
    protected void hookOnSubscribe(Subscription subscription) {
        this.subscription = subscription;
        request(1);
    }

    @Override
    protected void hookOnNext(UniversalIdIntQueueMessage message) {
        context = Context.of("message", message);

        ICommitOperation operation = PersistentQueue.deserialize(message.getContent());
        if (operation instanceof DocumentAddOperation) {
            prepareCommitAddition((IAddOperation) operation);
        } else {
            prepareCommitDeletion((IDeleteOperation) operation);
        }

        boolean finished = false;

        try {
            finished = processCommitOperation(operation);
        } catch (CommitterException e) {
            LOG.error(e);
        }

        if (finished) {
            // Let the queue know the message was used and can be deleted
            queue.finish(message);

            LOG.debug("Commit operation finished and removed from the ephemeral queue.");
        } else {
            LOG.info("Commit operation not finished and left on the ephemeral queue.");
        }

        request(1);
    }

    /**
     * Prepare the add operation before committing.
     * @param operation add operation
     */
    protected abstract void prepareCommitAddition(IAddOperation operation);

    /**
     *  Prepare the delete operation before committing.
     * @param operation delete operation
     */
    protected abstract void prepareCommitDeletion(IDeleteOperation operation);

    /**
     * @param operation
     * @return Returning true lets the queue know the commit operation was processed correctly, if
     *     not, return false and the operation will be left on the ephemeral queue for later
     *     processing.
     */
    protected abstract boolean processCommitOperation(ICommitOperation operation) throws CommitterException;

    @Override
    public void saveToXML(Writer out) throws IOException {
        try {
            EnhancedXMLStreamWriter writer = new EnhancedXMLStreamWriter(out);
            writer.writeStartElement("committerSubscriber");
            writer.writeAttribute("class", getClass().getCanonicalName());
            writer.writeAttributeString("xml:space", "preserve");

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
     * @param out the xml being written
     * @throws XMLStreamException problem saving to XML
     */
    protected abstract void saveToXML(XMLStreamWriter out) throws XMLStreamException;

    @Override
    public void loadFromXML(Reader reader) {
        XMLConfiguration xml = XMLConfigurationUtil.newXMLConfiguration(reader);
        loadFromXml(xml);
    }

    /**
     * Allows subclasses to load their config from xml
     *
     * @param xml XML configuration
     */
    protected abstract void loadFromXml(XMLConfiguration xml);

    @Override
    protected void hookOnComplete() {
        LOG.info("Complete.");
    }

    @Override
    protected void hookOnError(Throwable t) {
        queue.requeue(currentContext().get("message"));
    }

    @Override
    protected void hookFinally(SignalType type) {
        // NO-OP
    }

    @Override
    public Context currentContext() {
        return context;
    }
}
