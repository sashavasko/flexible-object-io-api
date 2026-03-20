package org.sv.flexobject.arrow;

import org.sv.flexobject.arrow.write.JsonFileWriter;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class ArrowJson {

    public static String toJsonString(VectorSchemaRoot root) throws IOException {
        String jsonString = "";
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            JsonFileWriter writer = new JsonFileWriter(out);
            writer.start(root.getSchema(), null);
            writer.write(root);
            writer.close();
            jsonString = new String(out.toByteArray());
        }
        return jsonString;
    }

    public static String toJsonString(ArrowRecordBatch batch) throws IOException {
//        String jsonString = "";
//        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
//            JsonFileWriter writer = new JsonFileWriter(out);
//            writer.start(root.getSchema(), null);
//            writer.writeBatch(batch.);
//            writer.close();
//            jsonString = new String(out.toByteArray());
//        }
//        return jsonString;
//
//        try (JsonFileWriter writer = new JsonFileWriter(new FileOutputStream("output.json"))) {
//            writer.start(root.getSchema());
//            writer.writeBatch(recordBatch);
//            writer.end();
//        } catch (IOException e) {
//            // Handle the exception
//        }
        return null;
    }
}
