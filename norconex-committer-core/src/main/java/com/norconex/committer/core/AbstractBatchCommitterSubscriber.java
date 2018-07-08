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
import java.util.ArrayList;
import java.util.List;

/**
 * This is a subscriber which can be used for batched message processing.
 */
public abstract class AbstractBatchCommitterSubscriber
        extends BaseSubscriber<List<UniversalIdIntQueueMessage>> implements ICommitterSubscriber, IXMLConfigurable {

    private static final Logger LOG = LogManager.getLogger(AbstractBatchCommitterSubscriber.class);

    private PersistentQueue queue;
    protected Subscription subscription;
    Context context = Context.empty();

    public AbstractBatchCommitterSubscriber() {

    }

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
    protected void hookOnNext(List<UniversalIdIntQueueMessage> msgs) {
        context = Context.of("messages", msgs);

        List<ICommitOperation> operations = new ArrayList<>();

        for (UniversalIdIntQueueMessage msg : msgs) {
            ICommitOperation operation = PersistentQueue.deserialize(msg.getContent());
            if (operation instanceof DocumentAddOperation) {
                prepareCommitAddition((IAddOperation) operation);
            } else {
                prepareCommitDeletion((IDeleteOperation) operation);
            }
            operations.add(operation);
        }

        boolean finished = false;

        try {
            finished = processCommitOperations(operations);
        } catch (CommitterException e) {
            LOG.error(e);
            throw new CommitterException("Commit operations not finished and put back to queue for later processing.");
        }

        if (finished) {
            for (UniversalIdIntQueueMessage msg : msgs) {
                // Let the queue know the message was used and can be deleted
                queue.finish(msg);
            }
            LOG.info("Commit operations finished and removed from the ephemeral queue.");
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
     * @param operations
     * @return Returning true lets the queue know the commit operations were processed correctly, if
     *     not, return false and the operations will be left on the ephemeral queue for later
     *     processing.
     */
    protected abstract boolean processCommitOperations(List<ICommitOperation> operations) throws CommitterException;

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
        LOG.error(t.getMessage());
        queue.requeuAll(currentContext().get("messages"));
    }

    @Override
    protected void hookFinally(SignalType type) {
        // NO-OP
    }

    @Override
    public Context currentContext() {
        return context;
    }

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
}
