package org.sv.flexobject.dremio.domain.catalog;


import org.sv.flexobject.StreamableImpl;

public class MetadataPolicy extends StreamableImpl {
    long authTTLMs; // Length of time to cache the privileges that the user has on the source, in milliseconds.
                    // For example, if authTTLMs is set to 28800000 (8 hours),
                    // Dremio checks the user's privilege status once every 8 hours.

    long namesRefreshMs; // How often to refresh the source, in milliseconds
    long datasetRefreshAfterMs; // How often to refresh the metadata in the source's datasets, in milliseconds.
    long datasetExpireAfterMs; // Maximum age to allow for the metadata in the source's datasets, in milliseconds.
    UpdateMode datasetUpdateMode;
    boolean deleteUnavailableDatasets; //If Dremio should remove dataset definitions from the source when the underlying data is unavailable, set to true
    boolean autoPromoteDatasets; // If Dremio should automatically format files into tables when a user issues a query, set to true.
    AccessControlList accessControlList;
}
