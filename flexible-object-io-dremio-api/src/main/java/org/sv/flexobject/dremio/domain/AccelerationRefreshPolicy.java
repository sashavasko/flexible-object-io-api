package org.sv.flexobject.dremio.domain;


import org.sv.flexobject.StreamableImpl;

public class AccelerationRefreshPolicy extends StreamableImpl {
    public Integer refreshPeriodMs; //Refresh period for the data in all reflections for the table, in milliseconds.
    public Integer gracePeriodMs; //Maximum age allowed for reflection data used to accelerate queries, in milliseconds.
    public AccelerationRefreshMethod method; //Approach used for refreshing the data in reflections for the table. For more information, read Refreshing Reflections.
    public String refreshField; //For INCREMENTAL refresh method, the field to refresh for the table. Used only if method is INCREMENTAL.
    public Boolean neverExpire; //If the reflection never expires, the value is true. Otherwise, the value is false.
    public Boolean neverRefresh; //If the reflection never refreshes, the value is true
}
