package org.elasticsearch.repositories.cos;

import java.security.BasicPermission;

public final class SpecialPermission extends BasicPermission {
    public static final org.elasticsearch.SpecialPermission INSTANCE = new org.elasticsearch.SpecialPermission();
    
    public SpecialPermission() {
        super("*");
    }
    
    public SpecialPermission(String name, String actions) {
        this();
    }
    
    public static void check() {
        SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            sm.checkPermission(INSTANCE);
        }
        
    }
}
