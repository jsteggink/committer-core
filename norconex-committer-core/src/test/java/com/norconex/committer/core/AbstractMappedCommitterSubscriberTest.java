/* Copyright 2010-2014 Norconex Inc.
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
import com.norconex.commons.lang.map.Properties;
import org.apache.commons.configuration.XMLConfiguration;
import org.junit.Before;
import org.junit.Test;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.IOException;
import java.util.List;

import static org.junit.Assert.*;

/** @author Jeroen Steggink */
@SuppressWarnings({"nls"})
public class AbstractMappedCommitterSubscriberTest {

    private StubCommitter committer;
    private StubCommitterSubscriber subscriber;
    private Properties metadata = new Properties();
    private String defaultReference = "1";
    private PersistentQueue queue;
    private String QUEUE_DIR = "committer-queue-test";
    private String QUEUE_NAME = "test";

    /**
     * Sets up a committer for testing.
     *
     * @throws IOException problem setting up committer
     */
    @Before
    public void setup() throws IOException {
        metadata.clear();
        metadata.addString("myreference", defaultReference);
    }

    /**
     * Test no commit if not enough document
     *
     * @throws IOException could not create temporary file
     */
    @Test
    public void testNoCommit() throws IOException {
        //committer.add(defaultReference, new NullInputStream(0), metadata);
        // assertFalse(committer.committed);
    }

    /** Test batch commit. */
    @Test
    public void testBatchCommit() throws InterruptedException {
        committer = new StubCommitter();
        committer.setQueueDir(QUEUE_DIR);
        committer.setQueueName(QUEUE_NAME);
        committer.setCommitBatchSize(2);

        committer.init();
        queue = committer.getQueue();

        committer.setSubscriber(
                new AbstractMappedCommitterSubscriber() {

                    @Override
                    protected void prepareCommitAddition(IAddOperation operation) {

                    }

                    @Override
                    protected void prepareCommitDeletion(IDeleteOperation operation) {

                    }

                    @Override
                    protected boolean processCommitOperations(List<ICommitOperation> operations) {
                        for (ICommitOperation operation : operations) {
                            if (!(operation instanceof DocumentAddOperation)) {
                                assertFalse(true);
                            }
                        }
                        // Processing went fine, return true.
                        return true;
                    }

                    @Override
                    protected void loadFromXml(XMLConfiguration xml) {

                    }

                    @Override
                    public void hookOnComplete() {
                        System.out.println("Complete");
                        assertEquals(0, queue.getQueueSize());
                        assertTrue(0 < queue.getEphemeralSize());
                    }
                });

        committer.add("1", null, metadata);
        committer.add("2", null, metadata);
        committer.add("3", null, metadata);
        committer.add("4", null, metadata);
        committer.add("5", null, metadata);
        committer.add("6", null, metadata);
        committer.add("7", null, metadata);
        committer.commit();
    }

    /** Test batch commit. */
    @Test
    public void testBatchCommitWithErrors() throws InterruptedException {
        committer = new StubCommitter();
        committer.setQueueDir(QUEUE_DIR);
        committer.setQueueName(QUEUE_NAME);
        committer.setCommitBatchSize(2);
        committer.init();
        queue = committer.getQueue();

        committer.setSubscriber(
                new AbstractBatchCommitterSubscriber() {

                    @Override
                    public void hookOnNext(List<UniversalIdIntQueueMessage> msgs) {
                        super.hookOnNext(msgs);
                        System.out.println("onNext");
                        throw new IllegalStateException("error");
                    }

                    @Override
                    protected void prepareCommitAddition(IAddOperation operation) {

                    }

                    @Override
                    protected void prepareCommitDeletion(IDeleteOperation operation) {

                    }

                    @Override
                    protected boolean processCommitOperations(List<ICommitOperation> operations) {
                        return false;
                    }

                    @Override
                    protected void loadFromXml(XMLConfiguration xml) {

                    }

                    @Override
                    protected void saveToXML(XMLStreamWriter out) throws XMLStreamException {

                    }

                    @Override
                    public void hookOnComplete() {
                        System.out.println("Complete");
                    }

                    @Override
                    public void hookOnError(Throwable t) {
                        assertTrue(queue.getEphemeralSize() > 0);
                        queue.requeuAll(currentContext().get("messages"));
                        assertEquals(0, queue.getEphemeralSize());
                    }
                });


        committer.add("1", null, metadata);
        committer.add("2", null, metadata);
        committer.add("3", null, metadata);
        committer.add("4", null, metadata);
        committer.add("5", null, metadata);
        committer.add("6", null, metadata);
        committer.add("7", null, metadata);
        committer.commit();
    }

    /**
     * Test setting the source and target IDs.
     *
     * @throws IOException could not create temporary file
     */
    @Test
    public void testSetSourceAndTargetReference() throws IOException {
        committer = new StubCommitter();
        committer.setQueueDir(QUEUE_DIR);
        committer.setQueueName(QUEUE_NAME);
        committer.init();

        // Set a different source and target id
        String customSourceId = "mysourceid";
        //committer.setSourceReferenceField(customSourceId);
        String customTargetId = "mytargetid";
        //committer.setTargetReferenceField(customTargetId);

        // Store the source id value in metadata
        metadata.addString(customSourceId, defaultReference);

        // Add a doc (it should trigger a commit because batch size is 1)
        committer.add(defaultReference, null, metadata);

        // Get the map generated
        // assertEquals(1, committer.getBatchSize());
        // IAddOperation op = (IAddOperation) committer.getCommitBatch().get(0);
        // Properties docMeta = op.getMetadata();

        // Check that customTargetId was used
        // assertEquals(defaultReference, docMeta.getString(customTargetId));

        // Check that customSourceId was removed (default behavior)
        // assertFalse("Source reference field was not removed.",
        //        docMeta.containsKey(customSourceId));
    }

    /**
     * Test keeping source id field.
     *
     * @throws IOException could not create temporary file
     */
    @Test
    public void testKeepSourceId() throws IOException {

        //committer.setKeepSourceReferenceField(true);

        // Add a doc (it should trigger a commit because batch size is 1)
        //committer.add(defaultReference, new NullInputStream(0), metadata);
        //committer.commit();

        // Get the map generated
        // assertEquals(1, committer.get);
        // IAddOperation op = (IAddOperation) committer.getCommitBatch().get(0);

        // Check that the source id is still there
        // assertTrue(op.getMetadata().containsKey("myreference"));
    }

    class StubCommitter extends BatchReactiveCommitter {

        @Override
        protected void saveToXML(XMLStreamWriter writer) throws XMLStreamException {

        }

        @Override
        protected void loadFromXml(XMLConfiguration xml) {

        }
    }

    class StubCommitterSubscriber extends AbstractMappedCommitterSubscriber {

        /**
         * Creates a new instance.
         *
         * @param queue
         */
        public StubCommitterSubscriber(PersistentQueue queue) {

        }

        @Override
        protected void prepareCommitAddition(IAddOperation operation) {

        }

        @Override
        protected void prepareCommitDeletion(IDeleteOperation operation) {

        }

        @Override
        protected boolean processCommitOperations(List list) {
            return false;
        }

        @Override
        protected void loadFromXml(XMLConfiguration xml) {
            // no loading
        }
    }
}
