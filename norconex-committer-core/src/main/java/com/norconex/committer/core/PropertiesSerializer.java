package com.norconex.committer.core;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.MapSerializer;
import com.norconex.commons.lang.map.Properties;

import java.util.HashMap;

public class PropertiesSerializer extends MapSerializer<Properties> {
    public void write (Kryo kryo, Output output, Properties properties) {
        Properties newProperties = new Properties(new HashMap(properties));
        super.write(kryo, output, newProperties);
    }

    public Properties read (Kryo kryo, Input input, Class<? extends Properties> type) {
        return new Properties(super.read(kryo, input, type));
    }
}

