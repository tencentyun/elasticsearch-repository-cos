package org.elasticsearch.repositories.cos;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;

import static org.elasticsearch.common.settings.Setting.*;

public class COSClientSettings {
    
    static {
        // Make sure repository plugin class is loaded before this class is used to trigger static initializer for that class which applies
        // necessary Jackson workaround
        try {
            Class.forName("org.elasticsearch.repositories.cos.COSRepositoryPlugin");
        } catch (ClassNotFoundException e) {
            throw new AssertionError(e);
        }
    }

    private static final String PREFIX = "cos.client.";

    private static final ByteSizeValue MIN_CHUNK_SIZE = new ByteSizeValue(1, ByteSizeUnit.MB);
    private static final ByteSizeValue MAX_CHUNK_SIZE = new ByteSizeValue(1, ByteSizeUnit.GB);

    public static final Setting<String> REGION =
            simpleString(PREFIX + "region", Property.NodeScope, Property.Dynamic);
    public static final Setting<SecureString> ACCESS_KEY_ID =
            new Setting<>(PREFIX + "access_key_id", "", SecureString::new,
                    Setting.Property.NodeScope, Setting.Property.Dynamic, Property.Filtered);
    public static final Setting<SecureString> ACCESS_KEY_SECRET =
            new Setting<>(PREFIX + "access_key_secret", "", SecureString::new,
                    Setting.Property.NodeScope, Setting.Property.Dynamic, Property.Filtered);
    public static final Setting<SecureString> APP_ID =
            new Setting<>(PREFIX + "app_id", "", SecureString::new,
                    Setting.Property.NodeScope, Setting.Property.Dynamic, Property.Filtered);
    public static final Setting<String> BUCKET =
            simpleString(PREFIX + "bucket", Setting.Property.NodeScope, Setting.Property.Dynamic);
    public static final Setting<String> BASE_PATH =
            simpleString(PREFIX + "base_path", "", Setting.Property.NodeScope, Setting.Property.Dynamic);
    public static final Setting<Boolean> COMPRESS =
            boolSetting(PREFIX + "compress", false, Setting.Property.NodeScope, Setting.Property.Dynamic);
    public static final Setting<ByteSizeValue> CHUNK_SIZE =
            byteSizeSetting(PREFIX + "chunk_size", MAX_CHUNK_SIZE, MIN_CHUNK_SIZE, MAX_CHUNK_SIZE,
                    Setting.Property.NodeScope, Setting.Property.Dynamic);
    public static final Setting<String> END_POINT = Setting.simpleString(PREFIX + "end_point", "", Property.NodeScope, Property.Dynamic);
}
