package org.sv.flexobject.dremio;

import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.sql.util.TableRef;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.lang3.StringUtils;
import org.sv.flexobject.connections.ConnectionManager;

import java.util.ArrayList;
import java.util.List;

public class DremioFlightDao implements AutoCloseable {

    DremioFlightSqlClient flightSqlClient;
    String connectionName;

    public DremioFlightDao(DremioFlightSqlClient flightSqlClient) {
        this.flightSqlClient = flightSqlClient;
    }

    public DremioFlightDao(String connectionName) {
        this.connectionName = connectionName;
    }

    public DremioFlightSqlClient getFlightSqlClient() {
        if (flightSqlClient == null && StringUtils.isNotBlank(connectionName)) {
            try {
                flightSqlClient = (DremioFlightSqlClient) ConnectionManager.getConnection(DremioFlightSqlClient.class, connectionName);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        if (flightSqlClient == null) {
            throw new RuntimeException("Arrow Flight sql client is not available");
        }
        return flightSqlClient;
    }

    @Override
    public void close() throws Exception {
        flightSqlClient.close();
    }

    public class CallBuilder {
        List<CallOption> callOptions = new ArrayList<>();
        String query;
        String catalog;
        String schema;
        String table;
        FlightInfo flightInfo;
        long updatedRows = 0;

        public CallBuilder option(CallOption option) {
            callOptions.add(option);
            return this;
        }

        public CallBuilder query(String query) {
            this.query = query;
            return this;
        }

        public CallBuilder catalog(String catalog) {
            this.catalog = catalog;
            return this;
        }

        public CallBuilder schema(String schema) {
            this.schema = schema;
            return this;
        }

        public CallBuilder table(String table) {
            this.table = table;
            return this;
        }

        protected CallOption[] getCallOptions() {
            return callOptions.toArray(new CallOption[0]);
        }

        public CallBuilder execute() throws Exception {
            flightInfo = getFlightSqlClient().execute(query, getCallOptions());
            return this;
        }

        public CallBuilder executeUpdate() {
            updatedRows = getFlightSqlClient().executeUpdate(query, getCallOptions());
            return this;
        }

        public CallBuilder getCatalogs() throws Exception {
            flightInfo = getFlightSqlClient().getCatalogs(getCallOptions());
            return this;
        }

        public CallBuilder getSchemas() throws Exception {
            flightInfo = getFlightSqlClient().getSchemas(catalog, schema, getCallOptions());
            return this;
        }

        public CallBuilder getTableTypes() throws Exception {
            flightInfo = getFlightSqlClient().getTableTypes(getCallOptions());
            return this;
        }

        public CallBuilder getTables() throws Exception {
            // For now, this won't filter by table types.
            flightInfo = getFlightSqlClient().getTables(catalog, schema, table, null, false, getCallOptions());
            return this;
        }

        public CallBuilder getExportedKeys() throws Exception {
            flightInfo = getFlightSqlClient().getExportedKeys(TableRef.of(catalog, schema, table), getCallOptions());
            return this;
        }

        public CallBuilder getImportedKeys() throws Exception {
            flightInfo = getFlightSqlClient().getImportedKeys(TableRef.of(catalog, schema, table), getCallOptions());
            return this;
        }

        public CallBuilder getPrimaryKeys() throws Exception {
            flightInfo = getFlightSqlClient().getPrimaryKeys(TableRef.of(catalog, schema, table), getCallOptions());
            return this;
        }

        public CallBuilder print() throws Exception {
            final FlightStream stream =
                    getFlightSqlClient().getStream(flightInfo.getEndpoints().get(0).getTicket(), getCallOptions());
            while (stream.next()) {
                try (final VectorSchemaRoot root = stream.getRoot()) {
                    Schema schema = root.getSchema();
                    System.out.println("Schema: " + schema + "\n");
                    for (Field f : schema.getFields()){
                        ArrowType type = f.getType();
//                        ArrowType.Struct
                        System.out.println("Field: "  + f.getName() + " Type:" + f.getType() + " Metadata:" + f.getMetadata());
                    }
//                    System.out.println("Data : \n" + root.contentToTSVString() + "\n");
                    List<FieldVector> fieldVectors = root.getFieldVectors();
                    int rowCount = root.getRowCount();
                    for (int i = 0 ; i < rowCount && i < 10 ; i++) {
                        for (FieldVector field : fieldVectors) {
                            Object value = field.getObject(i);
                            String fieldname = field.getName();
//                            field.get
                            if (value != null)
                                System.out.println("Field: " + fieldname + " class: " + value.getClass().getName() + " value: " + value);
                        }
                    }
                }
            }
            stream.close();
            return this;
        }
    }

    public CallBuilder call() {
        return new CallBuilder();
    }
}
