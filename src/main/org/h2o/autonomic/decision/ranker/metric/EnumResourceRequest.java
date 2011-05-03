package org.h2o.autonomic.decision.ranker.metric;

public enum EnumResourceRequest implements Resource {
    EXPECTED_TIME_TO_COMPLETE, //The time this action is expected to take in milliseconds.
    DISK_SPACE_NEEDED
    //The amount of disk space needed to complete the action (in KB).

}
