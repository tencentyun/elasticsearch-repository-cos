package org.elasticsearch.repositories.cos;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.common.settings.SecureSetting;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.AffixSetting;
import org.elasticsearch.common.settings.Settings;

public class COSClientSecretSettings {

    private final String account;
    private final String secretId;
    private final String secretKey;

    public String getAccount() {
        return account;
    }

    public String getSecretId() {
        return secretId;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public COSClientSecretSettings(String account, String secretId, String secretKey) {
        this.account = account;
        this.secretId = secretId;
        this.secretKey = secretKey;
    }

    // prefix for azure client settings
    private static final String COS_CLIENT_PREFIX_KEY = "qcloud.cos.client.";
    /** cos account name */
    public static final AffixSetting<SecureString> ACCOUNT_SETTING = Setting.affixKeySetting(
        COS_CLIENT_PREFIX_KEY,
        "account",
        key -> SecureSetting.secureString(key, null)
    );

    /** cos SecretId */
    public static final AffixSetting<SecureString> KEY_SETTING = Setting.affixKeySetting(
        COS_CLIENT_PREFIX_KEY,
        "secret_id",
        key -> SecureSetting.secureString(key, null)
    );

    /** cos SecretKey */
    public static final AffixSetting<SecureString> SECRET_SETTING = Setting.affixKeySetting(
        COS_CLIENT_PREFIX_KEY,
        "secret_key",
        key -> SecureSetting.secureString(key, null)
    );

    public static Map<String, COSClientSecretSettings> load(Settings settings) {
        final Map<String, COSClientSecretSettings> storageSettings = new HashMap<>();
        for (final String account : ACCOUNT_SETTING.getNamespaces(settings)) {
            storageSettings.put(account, getClientSettings(settings, account));
        }
        if (false == storageSettings.containsKey("cos_default") && false == storageSettings.isEmpty()) {
            // in case no setting named "cos_default" has been set, let's define our
            // "cos_default"
            final COSClientSecretSettings defaultSettings = storageSettings.values().iterator().next();
            storageSettings.put("cos_default", defaultSettings);
        }
        assert storageSettings.containsKey("cos_default") || storageSettings.isEmpty() : "always have 'cos_default' if any";
        return Collections.unmodifiableMap(storageSettings);
    }

    // pkg private for tests
    /** Parse settings for a single client. */
    private static COSClientSecretSettings getClientSettings(Settings settings, String account) {
        try (
            SecureString accountSecret = getConfigValue(settings, account, ACCOUNT_SETTING);
            SecureString secretIdSecret = getConfigValue(settings, account, KEY_SETTING);
            SecureString secretKeySecret = getConfigValue(settings, account, SECRET_SETTING)
        ) {
            return new COSClientSecretSettings(accountSecret.toString(), secretIdSecret.toString(), secretKeySecret.toString());
        }
    }

    private static <T> T getConfigValue(Settings settings, String account, Setting.AffixSetting<T> clientSetting) {
        final Setting<T> concreteSetting = clientSetting.getConcreteSettingForNamespace(account);
        return concreteSetting.get(settings);
    }
}
