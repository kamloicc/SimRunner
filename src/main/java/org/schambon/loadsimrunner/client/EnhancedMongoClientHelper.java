package org.schambon.loadsimrunner.client;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.net.ssl.SSLContext;
import java.io.FileInputStream;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import javax.net.ssl.TrustManagerFactory;
import java.util.ArrayList;
import java.util.Collection;

import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.UuidRepresentation;
import org.schambon.loadsimrunner.errors.InvalidConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.AutoEncryptionSettings;
import com.mongodb.ClientEncryptionSettings;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCompressor;
import com.mongodb.ServerApi;
import com.mongodb.ServerApiVersion;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.CreateEncryptedCollectionParams;
import com.mongodb.client.vault.ClientEncryption;
import com.mongodb.client.vault.ClientEncryptions;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Enhanced MongoClientHelper that supports both MongoDB Atlas (mongodb+srv://) 
 * and Amazon DocumentDB (mongodb://) connections with proper TLS configuration.
 */
public class EnhancedMongoClientHelper {

    private static final Logger LOGGER = LoggerFactory.getLogger(EnhancedMongoClientHelper.class);
    
    public static void doInTemporaryClient(String uri, TaskWithClient task) {
        try (var client = MongoClients.create(buildClientSettings(uri, null, null, null))) {
            task.run(client);
        }
    }

    public static void doInTemporaryClient(String uri, Document tlsConfig, TaskWithClient task) {
        try (var client = MongoClients.create(buildClientSettings(uri, tlsConfig, null, null))) {
            task.run(client);
        }
    }

    public static interface TaskWithClient {
        void run(MongoClient client);
    }

    /**
     * Creates a MongoClient with enhanced connection support for both MongoDB Atlas and DocumentDB
     * 
     * @param uri Connection string (mongodb:// for DocumentDB, mongodb+srv:// for MongoDB Atlas)
     * @param encryption Encryption configuration (optional)
     * @return Configured MongoClient
     */
    public static MongoClient client(String uri, Document encryption) {
        return client(uri, null, null, encryption);
    }

    public static MongoClient client(String uri, Document tlsConfig, Document encryption) {
        return client(uri, tlsConfig, null, encryption);
    }

    public static MongoClient client(String uri, Document tlsConfig, Document connectionPoolSettings, Document encryption) {
        MongoClientSettings settings = buildClientSettings(uri, tlsConfig, connectionPoolSettings, encryption);
        
        if (!isOn(encryption)) {
            return MongoClients.create(settings);
        } else {
            return createEncryptedClient(uri, tlsConfig, connectionPoolSettings, encryption, settings);
        }
    }

    private static MongoClientSettings buildClientSettings(String uri, Document tlsConfig, Document connectionPoolSettings, Document encryption) {
        ConnectionString connectionString = new ConnectionString(uri);
        MongoClientSettings.Builder settingsBuilder = MongoClientSettings.builder()
            .applyConnectionString(connectionString)
            .uuidRepresentation(UuidRepresentation.STANDARD)
            .serverApi(ServerApi.builder().version(ServerApiVersion.V1).build())
            .retryWrites(true)
            .retryReads(true);

        applyConnectionPoolSettings(settingsBuilder, connectionPoolSettings);

        if (isDocumentDBConnection(uri)) {
            applyDocumentDBSettings(settingsBuilder, tlsConfig);
        } else {
            if (tlsConfig != null) {
                applyCustomTLSSettings(settingsBuilder, tlsConfig);
            }
        }

        logClientConfiguration(uri, connectionPoolSettings);

        return settingsBuilder.build();
    }

    /**
     * Determines if the connection string is for DocumentDB
     */
    private static boolean isDocumentDBConnection(String uri) {
        return uri.startsWith("mongodb://") && 
               (uri.contains("docdb") || uri.contains("documentdb") || 
                uri.contains("amazonaws.com") || uri.contains("docdb-elastic"));
    }

    /**
     * Determines if the connection string is for MongoDB Atlas
     */
    private static boolean isMongoDBAtlasConnection(String uri) {
        return uri.startsWith("mongodb+srv://");
    }

    /**
     * Applies DocumentDB-specific TLS settings
     */
    private static void applyDocumentDBSettings(MongoClientSettings.Builder settingsBuilder, Document tlsConfig) {
        // DocumentDB requires TLS to be enabled
        settingsBuilder.applyToSslSettings(builder -> {
            builder.enabled(true);
            
            // Check for custom CA file path
            String tlsCAFile = getTLSCAFile(tlsConfig);
            if (tlsCAFile != null) {
                LOGGER.info("Using custom TLS CA file: {}", tlsCAFile);
                try {
                    // Create SSL context with custom CA file
                    SSLContext sslContext = createSSLContextWithCAFile(tlsCAFile);
                    builder.context(sslContext);
                } catch (Exception e) {
                    LOGGER.error("Failed to create SSL context with CA file: {}", tlsCAFile, e);
                    throw new InvalidConfigException("Failed to configure TLS with CA file: " + tlsCAFile + ". Error: " + e.getMessage());
                }
            }
            
            // DocumentDB doesn't support hostname verification in some configurations
            Boolean invalidHostNameAllowed = getInvalidHostNameAllowed(tlsConfig);
            if (invalidHostNameAllowed != null && invalidHostNameAllowed) {
                builder.invalidHostNameAllowed(true);
            }
        });
    }

    /**
     * Applies custom TLS settings for other connection types
     */
    private static void applyConnectionPoolSettings(MongoClientSettings.Builder settingsBuilder, Document poolConfig) {
        if (poolConfig == null) {
            LOGGER.debug("Using default connection pool settings");
            return;
        }

        Integer maxPoolSize = poolConfig.getInteger("maxPoolSize");
        Integer minPoolSize = poolConfig.getInteger("minPoolSize");
        Integer maxConnecting = poolConfig.getInteger("maxConnecting");
        Integer waitQueueTimeout = poolConfig.getInteger("waitQueueTimeout");
        Integer socketTimeout = poolConfig.getInteger("socketTimeout");
        List<String> compressors = poolConfig.getList("compressors", String.class);

        settingsBuilder.applyToConnectionPoolSettings(builder -> {
            if (maxPoolSize != null) {
                builder.maxSize(maxPoolSize);
            }
            if (minPoolSize != null) {
                builder.minSize(minPoolSize);
            }
            if (maxConnecting != null) {
                builder.maxConnecting(maxConnecting);
            }
            if (waitQueueTimeout != null) {
                builder.maxWaitTime(waitQueueTimeout, TimeUnit.MILLISECONDS);
            }
        });

        settingsBuilder.applyToSocketSettings(builder -> {
            if (socketTimeout != null) {
                builder.readTimeout(socketTimeout, TimeUnit.MILLISECONDS);
            }
        });

        if (compressors != null && !compressors.isEmpty()) {
            List<MongoCompressor> compressorList = compressors.stream()
                .map(c -> {
                    switch (c.toLowerCase()) {
                        case "snappy":
                            return MongoCompressor.createSnappyCompressor();
                        case "zstd":
                            return MongoCompressor.createZstdCompressor();
                        case "zlib":
                            return MongoCompressor.createZlibCompressor();
                        default:
                            LOGGER.warn("Unknown compressor: {}, ignoring", c);
                            return null;
                    }
                })
                .filter(c -> c != null)
                .collect(Collectors.toList());
            
            if (!compressorList.isEmpty()) {
                settingsBuilder.compressorList(compressorList);
            }
        }
    }

    private static void logClientConfiguration(String uri, Document poolConfig) {
        String maskedUri = uri.replaceAll("://[^@]+@", "://***:***@");
        
        LOGGER.info("MongoDB Client Configuration:");
        LOGGER.info("  Connection String: {}", maskedUri);
        LOGGER.info("  Server API: V1");
        LOGGER.info("  Retry Writes: true");
        LOGGER.info("  Retry Reads: true");

        if (poolConfig != null) {
            LOGGER.info("  Max Pool Size: {}", poolConfig.getInteger("maxPoolSize", 100));
            LOGGER.info("  Min Pool Size: {}", poolConfig.getInteger("minPoolSize", 0));
            LOGGER.info("  Max Connecting: {}", poolConfig.getInteger("maxConnecting", 2));
            LOGGER.info("  Wait Queue Timeout: {}ms", poolConfig.getInteger("waitQueueTimeout", 120000));
            LOGGER.info("  Socket Timeout: {}ms", poolConfig.getInteger("socketTimeout", 0));
            
            List<String> compressors = poolConfig.getList("compressors", String.class);
            if (compressors != null && !compressors.isEmpty()) {
                LOGGER.info("  Compressors: {}", compressors);
            } else {
                LOGGER.info("  Compressors: none");
            }
        } else {
            LOGGER.info("  Using default connection pool settings");
        }
    }

    private static void applyCustomTLSSettings(MongoClientSettings.Builder settingsBuilder, Document tlsConfig) {
        if (tlsConfig == null) return;

        Boolean tlsEnabled = tlsConfig.getBoolean("tls");
        if (tlsEnabled != null && tlsEnabled) {
            settingsBuilder.applyToSslSettings(builder -> {
                builder.enabled(true);
                
                String tlsCAFile = getTLSCAFile(tlsConfig);
                if (tlsCAFile != null) {
                    try {
                        SSLContext sslContext = createSSLContextWithCAFile(tlsCAFile);
                        builder.context(sslContext);
                    } catch (Exception e) {
                        throw new InvalidConfigException("Failed to configure TLS with CA file: " + tlsCAFile + ". Error: " + e.getMessage());
                    }
                }
                
                Boolean invalidHostNameAllowed = getInvalidHostNameAllowed(tlsConfig);
                if (invalidHostNameAllowed != null && invalidHostNameAllowed) {
                    builder.invalidHostNameAllowed(true);
                }
            });
        }
    }

    /**
     * Gets the TLS CA file path from configuration
     */
    private static String getTLSCAFile(Document tlsConfig) {
        if (tlsConfig == null) return null;
        return (String) getOrEnv(tlsConfig, "tlsCAFile");
    }

    /**
     * Gets the invalidHostNameAllowed setting from configuration
     */
    private static Boolean getInvalidHostNameAllowed(Document tlsConfig) {
        if (tlsConfig == null) return null;
        return tlsConfig.getBoolean("invalidHostNameAllowed");
    }

    /**
     * Creates SSL context with custom CA file (supports certificate bundles)
     */
    private static SSLContext createSSLContextWithCAFile(String caFilePath) throws Exception {
        try {
            // Load all CA certificates from the bundle file
            java.security.cert.CertificateFactory cf = java.security.cert.CertificateFactory.getInstance("X.509");
            java.io.FileInputStream fis = new java.io.FileInputStream(caFilePath);
            
            // Generate all certificates from the bundle
            java.util.Collection<? extends java.security.cert.Certificate> caCerts = cf.generateCertificates(fis);
            fis.close();
            
            LOGGER.info("Loaded {} certificates from CA bundle: {}", caCerts.size(), caFilePath);

            // Create a KeyStore containing all trusted CAs
            java.security.KeyStore keyStore = java.security.KeyStore.getInstance(java.security.KeyStore.getDefaultType());
            keyStore.load(null, null);
            
            // Add all certificates to the keystore
            int certIndex = 0;
            for (java.security.cert.Certificate cert : caCerts) {
                keyStore.setCertificateEntry("ca" + certIndex, cert);
                certIndex++;
            }

            // Create TrustManager that trusts all CAs in our KeyStore
            javax.net.ssl.TrustManagerFactory tmf = javax.net.ssl.TrustManagerFactory.getInstance(javax.net.ssl.TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(keyStore);

            // Create SSL context
            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(null, tmf.getTrustManagers(), null);
            
            LOGGER.info("Successfully created SSL context with {} CA certificates from: {}", caCerts.size(), caFilePath);
            return sslContext;
        } catch (Exception e) {
            LOGGER.error("Failed to create SSL context with CA file: {}", caFilePath, e);
            throw new Exception("Failed to load CA certificate bundle from: " + caFilePath + ". Error: " + e.getMessage(), e);
        }
    }

    /**
     * Creates an encrypted client with the given settings
     */
    private static MongoClient createEncryptedClient(String uri, Document tlsConfig, Document connectionPoolSettings, Document encryption, MongoClientSettings baseSettings) {
        var cryptSharedLibPath = getOrEnv(encryption, "sharedlib");
        var extraOptions = new HashMap<String, Object>();
        extraOptions.put("cryptSharedLibPath", cryptSharedLibPath);

        var kmsProviderCredentials = kmsProviderCredentials((Document) encryption.get("keyProviders"));

        var autoEncryptionSettings = AutoEncryptionSettings.builder()
            .keyVaultNamespace((String) getOrEnv(encryption, "keyVaultNamespace"))
            .kmsProviders(kmsProviderCredentials)
            .extraOptions(extraOptions)
            .build();

        var clientSettings = MongoClientSettings.builder(baseSettings)
            .autoEncryptionSettings(autoEncryptionSettings)
            .build();

        var encryptedClient = MongoClients.create(clientSettings);

        var keyVaultUri = encryption.getString("keyVaultUri");
        if (keyVaultUri == null) {
            keyVaultUri = uri;
        }

        // Build key vault client settings with same TLS configuration
        var keyVaultSettings = buildClientSettings(keyVaultUri, tlsConfig, connectionPoolSettings, null);
        var clientEncryptionSettings = ClientEncryptionSettings.builder()
            .keyVaultMongoClientSettings(keyVaultSettings)
            .keyVaultNamespace(encryption.getString("keyVaultNamespace"))
            .kmsProviders(kmsProviderCredentials)
            .build();
        var clientEncryption = ClientEncryptions.create(clientEncryptionSettings);

        for (var coll : encryption.getList("collections", Document.class)) {
            createEncryptedCollection(encryptedClient, clientEncryption, coll);
        }

        return encryptedClient;
    }

    private static void createEncryptedCollection(MongoClient encryptedClient, ClientEncryption clientEncryption,
            Document collDef) {
                
        String database = collDef.getString("database");
        String collection = collDef.getString("collection");
        String kmsProvider = collDef.getString("kmsProvider");
        var fields = (List) collDef.get("fields");

        if (database == null || collection == null || kmsProvider == null || fields == null) {
            throw new InvalidConfigException("Encrypted collection mandatory fields are database, collection, kmsProvider, fields");
        }

        var db = encryptedClient.getDatabase(database);
        if (collExists(db, collection)) {
            LOGGER.info("Collection {}.{} already exists, do not create encrypted collection", database, collection);
            return;
        }

        if (! collDef.containsKey("fields")) {
            throw new InvalidConfigException(String.format("Encrypted collection %s does not have a field map", collection));
        }

        Document fieldMap = new Document();
        fieldMap.put("fields", fields);

        CreateCollectionOptions opts = new CreateCollectionOptions().encryptedFields(fieldMap);
        CreateEncryptedCollectionParams params = new CreateEncryptedCollectionParams(kmsProvider);
        params.masterKey(new BsonDocument());  // TODO this will need to change for cloud KMS or KMIP

        try {
            clientEncryption.createEncryptedCollection(db, collection, opts, params);
        } catch (Exception e) {
            throw new RuntimeException("Cannot initialise encrypted collection", e);
        }
    }

    public static boolean collExists(MongoDatabase db, String collection) {
        var found = false;
        for (var name : db.listCollectionNames()) {
            if (name.equals(collection)) {
                found = true;
                break;
            }
        }
        return found;
    }

    private static Map<String, Map<String, Object>> kmsProviderCredentials(Document config) {
        Map<String, Map<String, Object>> result = new HashMap<>();

        for (var entry : config.entrySet()) {
            switch (entry.getKey()) {
                case "local":
                    result.put(entry.getKey(), _localKmsProviderCreds((Document) entry.getValue()));
                    break;
                default:
                    throw new UnsupportedOperationException(String.format("Key provider %s is not supported", entry.getKey()));
            }
        }

        return result;
    }

    private static Map<String, Object> _localKmsProviderCreds(Document config) {
        var keyPath = Paths.get(config.getString("key"));

        try (var fis = new FileInputStream(keyPath.toFile())) {
            byte[] cmk = new byte[96];
            if (fis.read(cmk) < 96) {
                throw new RuntimeException("Encryption keyfile should be 96 bytes");
            }

            Map<String, Object> keyMap = new HashMap<>();
            keyMap.put("key", cmk);
            return keyMap;
        } catch(IOException ioe) {
            throw new RuntimeException("Cannot read encryption keyfile", ioe);
        }
    }

    public static boolean isOn(Document config) {
        if (config == null) return false;
        else return config.getBoolean("enabled", false);
    }

    private static Object getOrEnv(Document doc, String key) {
        if (doc == null || !doc.containsKey(key)) {
            return null;
        } else {
            var found = doc.get(key);
            if (found instanceof String) {
                var string = (String) found;
                if (string.startsWith("$")) {
                    return System.getenv(string.substring(1));
                } else return string;
            } else {
                return found;
            }
        }
    }
}