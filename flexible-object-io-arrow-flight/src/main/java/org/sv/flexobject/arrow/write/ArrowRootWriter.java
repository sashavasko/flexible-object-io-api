package org.sv.flexobject.arrow.write;

import org.sv.flexobject.arrow.ArrowSchema;
import org.sv.flexobject.Streamable;
import org.sv.flexobject.schema.SchemaException;
import org.sv.flexobject.stream.Sink;
import org.sv.flexobject.utility.InstanceFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

public class ArrowRootWriter extends ArrowRecordWriter implements Sink<Streamable> {
    BufferAllocator allocator;
    VectorSchemaRoot root;

    public VectorSchemaRoot getRoot() {
        return root;
    }

    public ArrowRootWriter() {
    }

    protected ArrowRootWriter(Class<? extends Streamable> schemaClass, Schema arrowSchema, org.sv.flexobject.schema.Schema internalSchema) {
        super(schemaClass, arrowSchema.getFields(), internalSchema);
    }

    @Override
    public void setNull(int rowIndex) {

    }

    @Override
    public boolean put(Streamable value) throws Exception {
        return writeRecord(value);
    }

    @Override
    public boolean hasOutput() {
        return getRowCount() > 0;
    }

    @Override
    public void setEOF() {
        commit();
    }

    public static class Builder<SELF extends Builder> {
        Class <? extends Streamable> schemaClass;
        Schema arrowSchema;
        org.sv.flexobject.schema.Schema internalSchema;
        BufferAllocator rootAllocator;
        Class <? extends ArrowRootWriter> instanceOf = ArrowRootWriter.class;

        public SELF instanceOf(Class<? extends ArrowRootWriter> clazz) {
            instanceOf = clazz;
            return (SELF) this;
        }

        public SELF forClass(Class <? extends Streamable> schemaClass) {
            this.schemaClass = schemaClass;
            return (SELF) this;
        }

        public SELF withSchema(Schema schema) {
            this.arrowSchema = schema;
            return (SELF) this;
        }

        public SELF withInternalSchema(org.sv.flexobject.schema.Schema internalSchema) {
            this.internalSchema = internalSchema;
            return (SELF) this;
        }

        public SELF withRootAllocator(BufferAllocator allocator) {
            this.rootAllocator = allocator;
            return (SELF) this;
        }

        protected Object castToInstance (Object o){
            return instanceOf.cast(o);
        }

        public <O extends ArrowRootWriter> O build() throws ClassNotFoundException, NoSuchFieldException {

            if (schemaClass == null && internalSchema != null) {
                schemaClass = (Class<? extends Streamable>) getClass().getClassLoader().loadClass(internalSchema.getName());
            }
            if (schemaClass == null && arrowSchema != null) {
                if (arrowSchema.getCustomMetadata() != null) {
                    schemaClass = (Class<? extends Streamable>) getClass().getClassLoader().loadClass(arrowSchema.getCustomMetadata().get(("name")));
                }
            }
            if (schemaClass == null)
                throw new SchemaException("Source schema for writing is undefined");

            if (internalSchema == null)
                internalSchema = org.sv.flexobject.schema.Schema.getRegisteredSchema(schemaClass);

            if (arrowSchema == null)
                arrowSchema = ArrowSchema.forSchema(internalSchema);

            ArrowRootWriter writer = InstanceFactory.get(instanceOf);
            writer.setSchemaClass(this.schemaClass);
            writer.setFields(this.arrowSchema.getFields());
            writer.setInternalSchema(this.internalSchema);

            writer.allocator = rootAllocator == null ? new RootAllocator() : rootAllocator.newChildAllocator(schemaClass.getName() + "$write", 0, Long.MAX_VALUE);
            writer.root = VectorSchemaRoot.create(arrowSchema, writer.allocator);

            writer.buildFieldWriters();

            // TODO more stuff
            return (O) castToInstance(writer);
        }
    }

    public static Builder<Builder> builder() {
        return new Builder();
    }

    @Override
    protected FieldVector getFieldVector(String fieldName) {
        return root.getVector(fieldName);
    }

    public void commit(){
        root.setRowCount(getRowCount());
    }

    public void newBatch(){
        super.newBatch();
        root.clear();
    }

    @Override
    public void close() throws Exception {
        super.close();
        root.close();
        allocator.close();
    }
}
