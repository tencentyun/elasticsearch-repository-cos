/*
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may obtain
 * a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.repositories.cos;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;

import java.util.function.Function;

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

    /**
     * Placeholder client name for normalizing client settings in the repository settings.
     */
    private static final String PLACEHOLDER_CLIENT = "placeholder";

    private static final ByteSizeValue MIN_CHUNK_SIZE = new ByteSizeValue(1, ByteSizeUnit.MB);
    private static final ByteSizeValue MAX_CHUNK_SIZE = new ByteSizeValue(1, ByteSizeUnit.GB);

    static final Setting.AffixSetting<String> REGION = Setting.affixKeySetting(
            PREFIX,
            "region",
            key -> new Setting<>(key, "", Function.identity(), Property.NodeScope, Property.Dynamic)
    );

    static final Setting.AffixSetting<String> ACCESS_KEY_ID = Setting.affixKeySetting(
            PREFIX,
            "access_key_id",
            key -> new Setting<>(key, "", Function.identity(), Property.NodeScope, Property.Dynamic)
    );

    static final Setting.AffixSetting<String> ACCESS_KEY_SECRET = Setting.affixKeySetting(
            PREFIX,
            "access_key_secret",
            key -> new Setting<>(key, "", Function.identity(), Property.NodeScope, Property.Dynamic)
    );

    static final Setting.AffixSetting<String> APP_ID = Setting.affixKeySetting(
            PREFIX,
            "app_id",
            key -> new Setting<>(key, "", Function.identity(), Property.NodeScope, Property.Dynamic)
    );

    static final Setting.AffixSetting<String> BUCKET = Setting.affixKeySetting(
            PREFIX,
            "bucket",
            key -> new Setting<>(key, "", Function.identity(), Property.NodeScope, Property.Dynamic)
    );

    static final Setting.AffixSetting<String> BASE_PATH = Setting.affixKeySetting(
            PREFIX,
            "base_path",
            key -> new Setting<>(key, "", Function.identity(), Property.NodeScope, Property.Dynamic)
    );

    static final Setting.AffixSetting<Boolean> COMPRESS = Setting.affixKeySetting(
            PREFIX,
            "compress",
            key -> Setting.boolSetting(key, false, Property.NodeScope, Property.Dynamic)
    );

    static final Setting.AffixSetting<ByteSizeValue> CHUNK_SIZE = Setting.affixKeySetting(
            PREFIX,
            "chunk_size",
            key -> Setting.byteSizeSetting(key, MAX_CHUNK_SIZE, MIN_CHUNK_SIZE, MAX_CHUNK_SIZE, Property.NodeScope, Property.Dynamic)
    );

    static final Setting.AffixSetting<String> END_POINT = Setting.affixKeySetting(
            PREFIX,
            "end_point",
            key -> new Setting<>(key, "", Function.identity(), Property.NodeScope, Property.Dynamic)
    );

    static <T> T getConfigValue(Settings settings, AffixSetting<T> clientSetting) {
        // Normalize settings to placeholder client settings prefix so that we can use the affix settings directly
        Settings normalizedSettings = Settings.builder()
                .put(settings)
                .normalizePrefix(PREFIX + PLACEHOLDER_CLIENT + '.')
                .build();
        final Setting<T> concreteSetting = clientSetting.getConcreteSettingForNamespace(PLACEHOLDER_CLIENT);
        return concreteSetting.get(normalizedSettings);
    }
}
