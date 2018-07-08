package com.norconex.committer.core;

import com.esotericsoftware.kryo.KryoSerializable;
import com.norconex.commons.lang.map.Properties;
import org.apache.commons.io.IOUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Operation for adding a document {@link Document}.
 */
public class DocumentAddOperation implements IAddOperation {

    private Document document;

    DocumentAddOperation() {
    }

    DocumentAddOperation(Document document) {
        this.document = document;
    }

    @Override
    public String getReference() {
        return document.reference;
    }

    @Override
    public Properties getMetadata() {
        return document.metadata;
    }

    @Override
    public InputStream getContentStream() throws IOException {
        return new ByteArrayInputStream(document.content);
    }

    @Override
    public void delete() {
    }
}
