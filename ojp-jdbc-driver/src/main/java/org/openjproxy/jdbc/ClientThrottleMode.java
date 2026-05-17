package org.openjproxy.jdbc;

public enum ClientThrottleMode {
    OFF, PROACTIVE, REACTIVE, COMBINED;

    public static ClientThrottleMode fromString(String value) {
        if (value == null) {
            return COMBINED;
        }
        switch (value.trim().toUpperCase()) {
            case "OFF": return OFF;
            case "PROACTIVE": return PROACTIVE;
            case "REACTIVE": return REACTIVE;
            default: return COMBINED;
        }
    }
}
