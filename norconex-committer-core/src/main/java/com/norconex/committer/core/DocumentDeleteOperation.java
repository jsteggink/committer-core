package com.norconex.committer.core;

/**
 * Operation for deleting a document based on a reference.
 */
public class DocumentDeleteOperation implements IDeleteOperation {

    String reference;

    DocumentDeleteOperation() {

    }

    public DocumentDeleteOperation(String reference) {
        this.reference = reference;
    }

    @Override
    public String getReference() {
        return this.reference;
    }

    @Override
    public void delete() {
    }
}
