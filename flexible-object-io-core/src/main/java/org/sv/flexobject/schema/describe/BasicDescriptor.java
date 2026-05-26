package org.sv.flexobject.schema.describe;


import org.sv.flexobject.StreamableWithSchema;

public class BasicDescriptor<SELF extends BasicDescriptor> extends StreamableWithSchema {
    String displayName;
    String description;

    public BasicDescriptor() {
    }

    public BasicDescriptor(String displayName, String description) {
        this.displayName = displayName;
        this.description = description;
    }

    public SELF from(PropertyDescription propertyDescription) {
        if (propertyDescription != null) {
            this.displayName = propertyDescription.displayName();
            this.description = propertyDescription.description();
        }
        return (SELF) this;
    }

    public String getDisplayName() {
        return displayName;
    }

    public String getDescription() {
        return description;
    }
}
