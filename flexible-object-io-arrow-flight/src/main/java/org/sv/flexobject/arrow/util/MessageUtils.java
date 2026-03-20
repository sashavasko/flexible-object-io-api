package org.sv.flexobject.arrow.util;

import org.apache.arrow.flatbuf.Message;
import org.apache.arrow.flatbuf.MessageHeader;

public class MessageUtils {

    public static String headerType(Message message) {
        int type = message.headerType();
        if (type >= 0 && type < MessageHeader.names.length)
            return MessageHeader.name(type);
        return "Unknown(" + type + ")";
    }
}
