
package com.helicalinsight.adhoc.services;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;
import com.helicalinsight.datasource.GsonUtility;
import com.helicalinsight.datasource.nosql.NoSQLLoader;
import com.helicalinsight.efw.exceptions.EfwServiceException;
import com.mongodb.*;
import org.apache.commons.lang3.StringUtils;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;



import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import java.util.ArrayList;
import java.util.List;


/**
 * @author Somen
 * Created on 11/15/2017.
 */

@Component("com.helicalinsight.nosql.mongo")
@Scope("prototype")
@Deprecated
public class MongoDrillLoader extends NoSQLLoader {
    @Override
    public boolean loadToMiddleWare(JsonObject formDataJson) {
        JsonObject mongo = new JsonObject();
        String username = formDataJson.get("userName").getAsString();
        String password = formDataJson.get("password").getAsString();
        String jdbcUrl = GsonUtility.optString(formDataJson, "jdbcUrl");
        String host = getHostPort(jdbcUrl, true);
        String port = getHostPort(jdbcUrl, false);
        String storageName = formDataJson.get("name").getAsString();
        String theId = formDataJson.get("theId").getAsString();
        mongo.addProperty("type", "mongo");

        String connectionString = null;
        if (username.isEmpty() || password.isEmpty()) {

            connectionString = "mongodb://" + host + ":" + port;
        } else {
            connectionString = "mongodb://" + username + ":" + password + "@" + host + ":" + port + "/?authMechanism=SCRAM-SHA-1";
        }
        mongo.addProperty("connection", connectionString);
        mongo.addProperty("enabled", true);

        String drillStorageUrl = DrillCsvDataSourceCreator.getUrlOfDrill();

        String resourceUrl = drillStorageUrl + "/storage/" + storageName + "_" + theId + ".json";

        JsonObject storageJson = new JsonObject();
        storageJson.addProperty("name", storageName + "_" + theId);
        storageJson.add("config", mongo);

        String result = DrillCsvDataSourceCreator.drillRestApiCall(resourceUrl, "POST", storageJson.toString());
        if (result == null) {
            throw new EfwServiceException("There was some problem creating drill mongo connection");
        } else {
            try {
                JsonObject resultJSON = new Gson().fromJson(result,JsonObject.class);

            } catch (JsonSyntaxException e) {
                throw new EfwServiceException("There was a problem " + result);
            }
        }
        return true;
    }

    private String getHostPort(String uri, boolean isHost) {
        String splitArray[] = uri.split(":");
        if (splitArray.length >= 3) {
            if (isHost)
                return splitArray[1].replace("//", "");
            else
                return splitArray[2].substring(0, splitArray[2].indexOf("/"));
        }
        return "";
    }

    @Override
    public boolean testConnection(JsonObject formData) {
        String host = GsonUtility.optString(formData, "host");
        String uri = GsonUtility.optString(formData,"jdbcUrl");
        String database = GsonUtility.optString(formData,"database");
        String username = GsonUtility.optString(formData,"userName");
        String password = GsonUtility.optString(formData,"password");
        if (StringUtils.isEmpty(database)) {
            database = GsonUtility.optString(formData,"databaseName");
        }
        MongoModel mongoModel = new MongoModel();
        String splitArray[] = uri.split(":");
        if (splitArray.length >= 3) {
            String hostName = splitArray[1].replace("//", "");
            String port = splitArray[2].substring(0, splitArray[2].indexOf("/"));
            mongoModel.setHost(hostName + ":" + port);
            mongoModel.setUri(uri);
        }

        int timeout = GsonUtility.optInt(formData, "timeOut");
        int maxWait = GsonUtility.optInt(formData, "maxWait");
        String authMechanism = GsonUtility.optString(formData,"authMechanism");
        mongoModel.setDatabase(database);
        mongoModel.setUsername(username);
        mongoModel.setPassword(password);
        mongoModel.setAuthMechanism(authMechanism);
        mongoModel.setTimeout(timeout);
        mongoModel.setMaxWait(maxWait);
        return mongoModel.testConnection();
    }
}

class MongoModel {

    private String host;
    private String uri;
    private String database;
    private String username;
    private String password;
    private int timeout;
    private int maxWait;
    private String authMechanism;

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getUri() {
        return uri;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public int getTimeout() {
        return timeout;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    public int getMaxWait() {
        return maxWait;
    }

    public void setMaxWait(int maxWait) {
        this.maxWait = maxWait;
    }

    public String getAuthMechanism() {
        return authMechanism;
    }

    public void setAuthMechanism(String authMechanism) {
        this.authMechanism = authMechanism;
    }

    public String getSsl() {
        return ssl;
    }

    public void setSsl(String ssl) {
        this.ssl = ssl;
    }

    public String getKeyStorePath() {
        return keyStorePath;
    }

    public void setKeyStorePath(String keyStorePath) {
        this.keyStorePath = keyStorePath;
    }

    public String getKeyStorePassword() {
        return keyStorePassword;
    }

    public void setKeyStorePassword(String keyStorePassword) {
        this.keyStorePassword = keyStorePassword;
    }


    private String ssl;
    private String keyStorePath;
    private String keyStorePassword;

    public boolean testConnection() {
        MongoClient mongo = null;

        try {
            String targetDatabase = database == null ? "" : database.trim();
            String connectionUri = uri == null ? "" : uri.trim();

            if (!connectionUri.isEmpty()) {
                // Let the MongoDB driver parse the URI, including credentials,
                // options, and host information.
                MongoClientURI mongoURI = new MongoClientURI(connectionUri);
                mongo = new MongoClient(mongoURI);

                if (targetDatabase.isEmpty()) {
                    targetDatabase = mongoURI.getDatabase();
                }
            } else {
                // Host and database are mandatory for a direct connection.
                if (host == null || host.trim().isEmpty()
                        || targetDatabase.isEmpty()) {
                    return false;
                }

                MongoClientOptions.Builder builder =
                        MongoClientOptions.builder();

                if (timeout > 0) {
                    builder.connectTimeout(timeout);
                }

                if (maxWait > 0) {
                    builder.maxWaitTime(maxWait);
                }

                builder.socketKeepAlive(true);

                if (username != null && !username.trim().isEmpty()
                        && password != null) {
                    MongoCredential credential;

                    if ("MongoCR".equalsIgnoreCase(authMechanism)) {
                        credential = MongoCredential.createMongoCRCredential(
                                username, targetDatabase, password.toCharArray());
                    } else if ("ScramSha1".equalsIgnoreCase(authMechanism)) {
                        credential = MongoCredential.createScramSha1Credential(
                                username, targetDatabase, password.toCharArray());
                    } else if ("Plain".equalsIgnoreCase(authMechanism)) {
                        credential = MongoCredential.createPlainCredential(
                                username, targetDatabase, password.toCharArray());
                    } else {
                        credential = MongoCredential.createCredential(
                                username, targetDatabase, password.toCharArray());
                    }

                    mongo = new MongoClient(
                            new ServerAddress(host),
                            credential,
                            builder.build());
                } else {
                    mongo = new MongoClient(
                            new ServerAddress(host),
                            builder.build());
                }
            }

            if (targetDatabase == null || targetDatabase.trim().isEmpty()) {
                return false;
            }

            // Verify that the server actually responds to a database command.
            DB databaseHandle = mongo.getDB(targetDatabase);
            databaseHandle.command("ping");

            this.mongoDb = databaseHandle;
            return true;

        } catch (Exception e) {
            // The caller converts false into the application's connection
            // failure response. Do not expose credentials in error messages.
            return false;
        } finally {
            if (mongo != null) {
                mongo.close();
            }
        }

    }

    private boolean notNullOrBlank(String trustStorePassword) {
        return !((trustStorePassword == null) || (trustStorePassword.isEmpty()));
    }

    DB mongoDb;

}

