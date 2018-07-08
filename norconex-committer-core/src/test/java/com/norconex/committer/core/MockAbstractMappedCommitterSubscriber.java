package com.norconex.committer.core;

import org.apache.commons.configuration.XMLConfiguration;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.util.List;

public class MockAbstractMappedCommitterSubscriber extends AbstractMappedCommitterSubscriber {

    private boolean committed = false;

    /**
     * Creates a new instance.
     *
     */
    public MockAbstractMappedCommitterSubscriber() {
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
