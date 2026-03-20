package org.sv.flexobject.arrow.read;

import org.sv.flexobject.arrow.ArrowSchema;
import org.sv.flexobject.Streamable;
import org.sv.flexobject.schema.Schema;
import org.sv.flexobject.schema.SchemaException;
import org.sv.flexobject.stream.Source;
import org.sv.flexobject.utility.InstanceFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.commons.lang3.NotImplementedException;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class ArrowRootReader extends ArrowRecordReader implements Source<Streamable> {

    BufferAllocator allocator; // Optional - used if root is not set in builder
    VectorSchemaRoot root;

    public ArrowRootReader() {
    }

    protected ArrowRootReader(Class<? extends Streamable> schemaClass, List<Field> fields, Schema internalSchema) {
        super(schemaClass, fields, internalSchema);
    }

    public VectorSchemaRoot getRoot() {
        return root;
    }

    @Override
    public boolean isNull(int rowIndex) {
        return rowIndex >= root.getRowCount();
    }

    @Override
    public int getRowCount() {
        return root.getRowCount();
    }

    @Override
    public <O extends Streamable> O get() throws Exception {
        return readRecord();
    }

    @Override
    public boolean isEOF() {
        return !hasNext();
    }

    @Override
    public void close() throws Exception {
        Source.super.close();
        if (allocator != null) {
            root.close();
            allocator.close();
        }
    }

    @Override
    public Stream<Streamable> stream() {
        throw new NotImplementedException();
    }

    public static class Builder<SELF extends Builder> {
        Class <? extends Streamable> schemaClass;
        org.apache.arrow.vector.types.pojo.Schema arrowSchema;
        org.sv.flexobject.schema.Schema internalSchema;
        VectorSchemaRoot root;
        protected Map<Long, Dictionary> dictionaryMap;
        Class<? extends ArrowRootReader> instanceOf = ArrowRootReader.class;
        protected BufferAllocator rootAllocator;


        public SELF instanceOf(Class<? extends ArrowRootReader> instanceOf) {
            this.instanceOf = instanceOf;
            return (SELF) this;
        }

        public SELF withRootAllocator(BufferAllocator allocator) {
            this.rootAllocator = allocator;
            return (SELF) this;
        }

        public SELF forClass(Class <? extends Streamable> schemaClass) {
            this.schemaClass = schemaClass;
            return (SELF) this;
        }

        public SELF withSchema(org.apache.arrow.vector.types.pojo.Schema schema) {
            this.arrowSchema = schema;
            return (SELF) this;
        }

        public SELF withInternalSchema(org.sv.flexobject.schema.Schema internalSchema) {
            this.internalSchema = internalSchema;
            return (SELF) this;
        }

        public SELF withRoot(VectorSchemaRoot root) {
            this.root = root;
            return (SELF) this;
        }

        public SELF withDictionary(Map<Long, Dictionary> dictionaryMap) {
            this.dictionaryMap = dictionaryMap;
            return (SELF) this;
        }

        protected Object castToInstance (Object o){
            return instanceOf.cast(o);
        }

        public <O extends ArrowRootReader> O build() throws ClassNotFoundException, NoSuchFieldException {

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

            ArrowRootReader reader = InstanceFactory.get(instanceOf);
            reader.setSchemaClass(schemaClass);
            reader.setInternalSchema(internalSchema);
            reader.setFields(arrowSchema.getFields());

            if (this.root != null) {
                reader.root = this.root;
            } else {
                reader.allocator = rootAllocator == null ? new RootAllocator() : rootAllocator.newChildAllocator(schemaClass.getName() + "$read", 0, Long.MAX_VALUE);
                reader.root = VectorSchemaRoot.create(arrowSchema, reader.allocator);
            }

            reader.setDictionaryMap(this.dictionaryMap);

            reader.buildFieldReaders();

            return (O)castToInstance(reader);
        }

        public <O extends Source> O buildSource() throws Exception{
            return (O)build();
        }
    }

    public static Builder<Builder> builder() {
        return new Builder();
    }

    @Override
    protected FieldVector getFieldVector(String fieldName) {
        return root.getVector(fieldName);
    }

    public boolean hasNext(){
        return getRowCount() > getRowIndex();
    }

    public BufferAllocator getAllocator() {
        return allocator;
    }
}
