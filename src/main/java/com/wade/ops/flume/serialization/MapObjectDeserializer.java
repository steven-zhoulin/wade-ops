package com.wade.ops.flume.serialization;

import com.google.common.collect.Lists;
import com.wade.ops.flume.FlumeInputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.serialization.EventDeserializer;
import org.apache.flume.serialization.ResettableInputStream;

import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class MapObjectDeserializer implements EventDeserializer {

    private static final Log LOG = LogFactory.getLog(MapObjectDeserializer.class);

    private final ResettableInputStream in;
    private ObjectInputStream ois;
    private final Charset outputCharset;
    private final int maxLineLength;
    private volatile boolean isOpen;

    public static final String OUT_CHARSET_KEY = "outputCharset";
    public static final String CHARSET_DFLT = "UTF-8";

    public static final String MAXLINE_KEY = "maxLineLength";
    public static final int MAXLINE_DFLT = 2048;

    public MapObjectDeserializer(Context context, ResettableInputStream in) {

        this.in = in;
        this.outputCharset = Charset.forName(context.getString(OUT_CHARSET_KEY, CHARSET_DFLT));
        this.maxLineLength = context.getInteger(MAXLINE_KEY, MAXLINE_DFLT);
        this.isOpen = true;
        try {
            this.ois = new ObjectInputStream(new FlumeInputStream(this.in));
        } catch (IOException e) {
            LOG.error("create ObjectInputStream failed: " + e.getMessage());
            this.isOpen = false;
        }

    }

    /**
     * Reads a Map Object from a file and returns an event
     *
     * @return Event containing parsed Map Object
     * @throws IOException
     */
    @Override
    public Event readEvent() throws IOException {
        ensureOpen();
        Map<String, Object> o = readMapObject();
        if (null == o) {
            return null;
        } else {
            return EventBuilder.withBody(o.toString(), outputCharset);
        }
    }

    /**
     * Batch map object read
     *
     * @param numEvents Maximum number of events to return.
     * @return List of events containing map objects
     * @throws IOException
     */
    @Override
    public List<Event> readEvents(int numEvents) throws IOException {
        ensureOpen();
        List<Event> events = Lists.newLinkedList();
        for (int i = 0; i < numEvents; i++) {
            Event event = readEvent();
            if (event != null) {
                events.add(event);
            } else {
                break;
            }
        }
        return events;
    }

    @Override
    public void mark() throws IOException {
        ensureOpen();
        in.mark();
    }

    @Override
    public void reset() throws IOException {
        ensureOpen();
        in.reset();
    }

    @Override
    public void close() throws IOException {
        if (isOpen) {
            reset();
            in.close();
            ois.close();
            isOpen = false;
        }
    }

    private void ensureOpen() {
        if (!isOpen) {
            throw new IllegalStateException("Serializer has been closed");
        }
    }

    private Map<String, Object> readMapObject() throws IOException {

        Map<String, Object> rtn = null;

        try {
            rtn = (Map<String, Object>) ois.readObject();
        } catch (EOFException e) {
            LOG.error(e.getMessage());
            close();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            close();
        }

        return rtn;

    }

    public static class Builder implements EventDeserializer.Builder {

        @Override
        public EventDeserializer build(Context context, ResettableInputStream in) {
            return new MapObjectDeserializer(context, in);
        }

    }

}