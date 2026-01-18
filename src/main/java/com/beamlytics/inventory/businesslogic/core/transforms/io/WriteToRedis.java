package com.beamlytics.inventory.businesslogic.core.transforms.io;

import org.apache.beam.sdk.io.redis.RedisIO;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

public class WriteToRedis extends PTransform<PCollection<KV<String, String>>, PDone> {
    private final String host;
    private final int port;

    public WriteToRedis(String host, int port) {
        this.host = host;
        this.port = port;
    }

    @Override
    public PDone expand(PCollection<KV<String, String>> input) {
        return input.apply(RedisIO.write().withEndpoint(host, port));
    }
}
