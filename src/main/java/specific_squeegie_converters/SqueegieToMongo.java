package specific_squeegie_converters;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;
import com.mongodb.util.JSON;
import db.DBConnection;
import db.Sources;
import org.apache.commons.io.IOUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Created by AmyBear on 5/8/14.
 */
public class SqueegieToMongo {

    /* Instance variables */
    private static DBConnection _db;
    private static DBCollection _coll;
    private static String _filename;

    public SqueegieToMongo(String filename) {
        _db = new DBConnection("activities");
        _coll = _db.getCollection();
        _filename = filename;
    }

    public static void doSqueeging() {
        try {
            JSONArray array = new JSONArray(IOUtils.toString(new FileInputStream("src/main/java/squeegie_output/" + _filename)));
            for (int i = 0; i < array.length(); i++) {
                JSONObject object = array.getJSONObject(i);
                if (object.has("title")) {
                    BasicDBObject query = getQuery(object);
                    BasicDBObject document = parseJSON(object);
                    BasicDBObject metadata = parseMetadata(object);
                    if (query != null && document != null) {
                        if (_coll.findOne(query) != null) {
                            DBObject found = _coll.findOne(query);
                            _coll.update(query, new BasicDBObject("$addToSet", new BasicDBObject("metadata", metadata)));

                            BasicDBObject set = new BasicDBObject();

                            if (!found.containsField("latitude") && document.containsField("latitude"))
                                set.append("$set", new BasicDBObject("latitude", document.getString("latitude")));
                            if (!found.containsField("longitude") && document.containsField("longitude"))
                                set.append("$set", new BasicDBObject("longitude", document.getString("longitude")));
                            if (!found.containsField("phone") && document.containsField("phone"))
                                set.append("$set", new BasicDBObject("phone", document.getString("phone")));
                            if (!found.containsField("address") && document.containsField("address"))
                                set.append("$set", new BasicDBObject("address", document.getString("address")));
                            if (!found.containsField("website") && document.containsField("website"))
                                set.append("$set", new BasicDBObject("website", document.getString("website")));

                            if (Arrays.asList(Sources.restaurantJSONFiles).contains(_filename)) { // Sets the type of activity
                                set.append("$set", new BasicDBObject("type", "restaurant"));
                            } else if (Arrays.asList(Sources.otherJSONFiles).contains(_filename)) {
                                Object type = metadata.get("type");
                                if (type instanceof JSONArray) {
                                    set.append("$set", new BasicDBObject("type", JSON.parse(type.toString())));
                                } else {
                                    set.append("$set", new BasicDBObject("type", type));
                                }
                            }
                            _coll.update(query, set);

                        } else {
                            if (Arrays.asList(Sources.restaurantJSONFiles).contains(_filename)) {
                                document.append("type", "restaurant");
                            } else if (Arrays.asList(Sources.otherJSONFiles).contains(_filename)) {
                                Object type = metadata.get("type");
                                if (type instanceof JSONArray) {
                                    document.append("type", JSON.parse(type.toString()));
                                } else {
                                    document.append("type", type);
                                }
                            }

                            _coll.insert(document);
                            _coll.update(query, new BasicDBObject("$addToSet", new BasicDBObject("metadata", metadata)));
                        }

                        System.out.println("One updated: " + object.getString("title"));
                    } else {
                        System.out.println("Something went wrong with either the query or the document, or both");
                    }
                }
            }

        } catch (JSONException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("All done!");
    }

    public static BasicDBObject getQuery(JSONObject object) {
        try {
            if (object.has("address") && object.has("lat") && object.has("lng")) {
                String address = object.getString("address");
                address = address.replaceAll("\\.", "");
                address = address.replaceAll("\\,", "");
                address = address.toLowerCase();

                DBObject query = QueryBuilder.start("title").is(object.getString("title")).or(
                        QueryBuilder.start("address").is(address).get(),
                        QueryBuilder.start().and("latitude").is(object.getDouble("lat")).and("longitude").is(object.getDouble("lng")).get()
                ).get();
                return (BasicDBObject) query;
            } else {
                BasicDBObject query = new BasicDBObject("title", object.getString("title"));
                if (object.has("address")) {
                    String address = object.getString("address");
                    address = address.replaceAll("\\.", "");
                    address = address.replaceAll("\\,", "");
                    address = address.toLowerCase();
                    query.append("address", address);
                } else if (object.has("lat") && object.has("lng")) {
                    query.append("latitude", object.getDouble("lat"));
                    query.append("longitude", object.getDouble("lng"));
                }
                return query;
            }
        } catch (JSONException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static BasicDBObject parseJSON(JSONObject object) {
        BasicDBObject document = null;
        try {
            document = new BasicDBObject(); // Document to insert into Mongo
            if (object.has("title")) document.append("title", object.getString("title"));
            if (object.has("phone")) document.append("phone", object.getString("phone"));
            if (object.has("address")) {
                String address = object.getString("address");
                address = address.replaceAll("\\.", "");
                address = address.replaceAll("\\,", "");
                address = address.toLowerCase();
                document.append("address", address);
            }
            if (object.has("website")) document.append("website", object.getString("website"));
            if (object.has("lat")) document.append("latitude", object.getDouble("lat"));
            if (object.has("lng")) document.append("longitude", object.getDouble("lng"));

        } catch (JSONException e) {
            e.printStackTrace();
        }
        return document;
    }

    public static boolean keyIsNotOfMainType(String key) {
        if (key.equals("title") || key.equals("address") || key.equals("lat") || key.equals("lng") || key.equals("phone") || key.equals("website")) {
            return false;
        }
        return true;
    }

    public static BasicDBObject parseMetadata(JSONObject object) throws JSONException {
        BasicDBObject metadataValue = new BasicDBObject();
        Iterator<?> keys = object.keys();
        while (keys.hasNext()) {
            String key = (String) keys.next();
            Object obj = object.get(key);
            if (keyIsNotOfMainType(key)) {
                if (obj instanceof JSONArray) {
                    metadataValue.append(key, JSON.parse(obj.toString()));
                } else {
                    metadataValue.append(key, obj);
                }
            }
        }
        if (!metadataValue.containsField("source")) {
            String[] resource = _filename.split("\\-");
            metadataValue.append("source", resource[0]);
        }
        return metadataValue;
    }

}
