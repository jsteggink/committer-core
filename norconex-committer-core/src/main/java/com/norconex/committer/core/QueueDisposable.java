package com.norconex.committer.core;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import reactor.core.Disposable;

/**
 * This class can be used to add a dispose action.
 */
public class QueueDisposable implements Disposable {

    private static final Logger LOG = LogManager.getLogger(QueueDisposable.class);

    @Override
    public void dispose() {
        LOG.info("Dispose.");
    }
}
