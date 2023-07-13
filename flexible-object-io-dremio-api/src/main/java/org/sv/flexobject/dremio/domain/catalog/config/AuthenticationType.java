package org.sv.flexobject.dremio.domain.catalog.config;

public enum AuthenticationType {
    ANONYMOUS,      // No authentication is needed.
    MASTER,         // Use credentials from a master database user or use a secret resource URL
    KERBEROS        // Authenticate with Kerberos.
}
