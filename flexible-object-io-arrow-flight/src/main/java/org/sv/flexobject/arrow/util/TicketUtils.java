package org.sv.flexobject.arrow.util;

import org.apache.arrow.flight.Ticket;
import org.apache.commons.codec.binary.Hex;

public class TicketUtils {

    public static String toString(Ticket ticket){
        return Hex.encodeHexString(ticket.getBytes());
    }
}
