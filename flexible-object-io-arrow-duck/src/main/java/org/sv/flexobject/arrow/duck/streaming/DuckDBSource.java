package org.sv.flexobject.arrow.duck.streaming;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.duckdb.DuckDBConnection;
import org.duckdb.DuckDBResultSet;
import org.sv.flexobject.Streamable;
import org.sv.flexobject.arrow.read.ArrowRootReader;
import org.sv.flexobject.connections.ConnectionManager;
import org.sv.flexobject.util.AutoCloseables;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.function.Consumer;

public class DuckDBSource <T extends Streamable> extends ArrowRootReader {

    DuckDBConnection connection;
    PreparedStatement preparedStatement;
    DuckDBResultSet resultSet;
    ArrowReader arrowReader;

    public static class DuckDBSourceBuilder<SELF extends DuckDBSourceBuilder<?>> extends Builder<SELF> {

        Class<? extends Streamable> schema;

        DuckDBConnection connection;
        PreparedStatement preparedStatement;

        public DuckDBSourceBuilder() {
            instanceOf(DuckDBSource.class);
        }

        public SELF connection(String connectionName) throws Exception {
            this.connection = ConnectionManager.getConnection(DuckDBConnection.class, connectionName);
            return (SELF) this;
        }

        public SELF dbPath(String dbPath) throws SQLException, ClassNotFoundException {
            this.connection = DuckDBConnectionProvider.getConnection(dbPath);
            return (SELF) this;
        }

        public SELF query(String query) throws SQLException {
            if (this.connection == null)
                throw new IllegalArgumentException("Please specify either the connection name or database path prior to query");
            preparedStatement = this.connection.prepareStatement(query);
            return (SELF) this;
        }

        public SELF withParams(Consumer<PreparedStatement> setter) {
            if (this.preparedStatement == null)
                throw new IllegalArgumentException("Please specify query prior to setting params");
            setter.accept(this.preparedStatement);
            return (SELF)this;
        }


        @Override
        public <O extends ArrowRootReader> O build() throws ClassNotFoundException, NoSuchFieldException {

            DuckDBResultSet resultset;
            ArrowReader arrowReader;
            try {
                resultset = (DuckDBResultSet) preparedStatement.executeQuery();
                arrowReader = (ArrowReader) resultset.arrowExportStream(new RootAllocator(), 256);
                arrowReader.loadNextBatch();
                withRoot(arrowReader.getVectorSchemaRoot());
                withDictionary(arrowReader.getDictionaryVectors());
            } catch (Exception e){
                throw new RuntimeException("Failed to execute DuckDB query", e);
            }

            DuckDBSource<?> source = super.build();

            source.connection = this.connection;
            source.preparedStatement = this.preparedStatement;
            source.resultSet = resultset;
            source.arrowReader = arrowReader;

            return (O) castToInstance(source);
        }
    }

    public static DuckDBSourceBuilder<?> sourceBuilder(){
        return new DuckDBSourceBuilder<>();
    }

    @Override
    public boolean hasNext() {
        if (super.hasNext())
            return true;
        try {
            if (arrowReader.loadNextBatch()) {
                reset();
                return true;
            }
        } catch (IOException e) {
            return false;
        }
        return false;
    }

    @Override
    public void close() throws Exception {
        super.close();
        AutoCloseables.close(arrowReader, resultSet, preparedStatement, connection);
    }
}
