package com.norconex.committer.core;

import java.io.InputStream;
import java.io.Serializable;

import com.esotericsoftware.kryo.serializers.DefaultSerializers;
import com.esotericsoftware.kryo.serializers.FieldSerializer;
import com.norconex.commons.lang.map.Properties;

/**
 * Document contains the reference, metadata and content.
 */
public class Document implements Serializable {

    @FieldSerializer.Bind(serializer = DefaultSerializers.StringSerializer.class, valueClass = String.class, canBeNull = false)
    String reference;

    byte[] content = null;

    @FieldSerializer.Bind(serializer = PropertiesSerializer.class, valueClass = Properties.class, canBeNull = true)
    Properties metadata = null;

    Document() {}

    public Document(String reference) {
        this.reference = reference;
    }

    public Document(String reference, Properties metadata) {
        this.reference = reference;
        this.metadata = metadata;
    }

    public Document(String reference, byte[] content, Properties metadata) {
        this.reference = reference;
        this.content = content;
        this.metadata = metadata;
    }

    @Override
    public String toString() {
        return "Document{" +
                "reference='" + reference + '\'' +
                ", content=" + content +
                ", metadata=" + metadata +
                '}';
    }
}
