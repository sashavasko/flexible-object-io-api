package org.sv.flexobject.arrow.vortex.streaming;

import dev.vortex.api.Session;

public class VortexSession {

    private static final Session session = Session.create();

    public static Session get(){
        return session;
    }
}
