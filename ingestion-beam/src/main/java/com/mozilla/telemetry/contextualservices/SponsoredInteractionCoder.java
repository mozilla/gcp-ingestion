package com.mozilla.telemetry.contextualservices;

import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.values.TypeDescriptor;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

public class SponsoredInteractionCoder extends CustomCoder<SponsoredInteraction> {
    private static final Coder<Map<String, String>> MAP_CODER = NullableCoder.of(MapCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()));

    public SponsoredInteractionCoder() {
    }

    public static SponsoredInteractionCoder of(TypeDescriptor<SponsoredInteraction> ignored) {
        return of();
    }

    public static SponsoredInteractionCoder of() {
        return new SponsoredInteractionCoder();
    }

    public void encode(SponsoredInteraction value, OutputStream outStream) throws IOException {
        this.encode(value, outStream, Context.NESTED);
    }

    public void encode(SponsoredInteraction value, OutputStream outStream, Context context) throws IOException {
        MAP_CODER.encode(value.toMap(), outStream, context);
    }

    public SponsoredInteraction decode(InputStream inStream) throws IOException {
        return this.decode(inStream, Context.NESTED);
    }

    public SponsoredInteraction decode(InputStream inStream, Context context) throws IOException {
        Map<String, String> map = (Map)MAP_CODER.decode(inStream, context);
        return SponsoredInteraction.of(map);
    }
}
