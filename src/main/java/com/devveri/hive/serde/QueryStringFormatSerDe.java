package com.devveri.hive.serde;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.*;

/**
 * This SerDe parses query string formatted file and returns as a map
 * Based on JSONSerDe
 *
 * @author hakanilter
 */
public class QueryStringFormatSerDe implements SerDe {

    private static final Logger LOG;

    static {
        LOG = LoggerFactory.getLogger(QueryStringFormatSerDe.class);
    }

    // The number of columns in the table this SerDe is being used with
    private int numColumns;

    // List of column names in the table
    private List<String> columnNames;

    // An ObjectInspector to be used as meta-data about a deserialized row
    private StructObjectInspector rowOI;

    // List of row objects
    private ArrayList<Object> row;

    // List of column type information
    private List<TypeInfo> columnTypes;

    // Initialize this SerDe with the system properties and table properties
    @Override
    public void initialize(Configuration sysProps, Properties tblProps) throws SerDeException {
        LOG.debug("Initializing QueryStringSerDe");

        // Get the names of the columns for the table this SerDe is being used
        // with
        String columnNameProperty = tblProps.getProperty(serdeConstants.LIST_COLUMNS);
        columnNames = Arrays.asList(columnNameProperty.split(","));

        // Convert column types from text to TypeInfo objects
        String columnTypeProperty = tblProps.getProperty(serdeConstants.LIST_COLUMN_TYPES);
        columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);
        assert columnNames.size() == columnTypes.size();
        numColumns = columnNames.size();

        // Create ObjectInspectors from the type information for each column
        List<ObjectInspector> columnOIs = new ArrayList<ObjectInspector>(columnNames.size());
        ObjectInspector oi;
        for (int c = 0; c < numColumns; c++) {
            oi = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(columnTypes.get(c));
            columnOIs.add(oi);
        }
        rowOI = ObjectInspectorFactory.getStandardStructObjectInspector(columnNames, columnOIs);

        // Create an empty row object to be reused during deserialization
        row = new ArrayList<Object>(numColumns);
        for (int c = 0; c < numColumns; c++) {
            row.add(null);
        }

        LOG.debug("QueryStringSerDe initialization complete");
    }

    // Gets the ObjectInspector for a row deserialized by this SerDe
    @Override
    public ObjectInspector getObjectInspector() throws SerDeException {
        return rowOI;
    }

    // Deserialize object into a row for the table
    @Override
    public Object deserialize(Writable blob) throws SerDeException {
        Text rowText = (Text) blob;
        String[] values = rowText.toString().split("\t");
        String queryString = values[1];

        if (LOG.isDebugEnabled()) {
            LOG.debug("Deserialize row: " + queryString);
        }

        // create a map from log string
        Map<String, String> map = getQueryMap(queryString);
        map.put("key", values[0]);

        // Loop over columns in table and set values
        String colName;
        Object value;
        for (int c = 0; c < numColumns; c++) {
            colName = columnNames.get(c).toLowerCase();
            TypeInfo ti = columnTypes.get(c);

            if (!map.containsKey(colName)) {
                value = null;
            } else if (ti.getTypeName().equalsIgnoreCase(serdeConstants.DOUBLE_TYPE_NAME)) {
                value = Double.parseDouble(map.get(colName));
            } else if (ti.getTypeName().equalsIgnoreCase(serdeConstants.BIGINT_TYPE_NAME)) {
                value = Long.parseLong(map.get(colName));
            } else if (ti.getTypeName().equalsIgnoreCase(serdeConstants.INT_TYPE_NAME)) {
                value = Integer.parseInt(map.get(colName));
            } else if (ti.getTypeName().equalsIgnoreCase(serdeConstants.TINYINT_TYPE_NAME)) {
                value = Byte.parseByte(map.get(colName));
            } else if (ti.getTypeName().equalsIgnoreCase(serdeConstants.FLOAT_TYPE_NAME)) {
                value = Float.parseFloat(map.get(colName));
            } else if (ti.getTypeName().equalsIgnoreCase(serdeConstants.BOOLEAN_TYPE_NAME)) {
                value = Boolean.parseBoolean(map.get(colName));
            } else {
                value = decode(map.get(colName));
            }
            row.set(c, value);
        }

        return row;
    }

    @Override
    public Class<? extends Writable> getSerializedClass() {
        return Text.class;
    }

    // Serializes a row of data into a query string
    @Override
    public Writable serialize(Object obj, ObjectInspector objInspector) throws SerDeException {
        LOG.info(obj.toString());
        LOG.info(objInspector.toString());

        return null;
    }

    @Override
    public SerDeStats getSerDeStats() {
        return null;
    }

    // Parses query string into a map
    private Map<String, String> getQueryMap(String query) {
        String[] params = query.split("&");
        Map<String, String> map = new HashMap<String, String>();
        for (String param : params) {
            String[] values = param.split("=");
            if (values.length == 2) {
                String name = values[0].toLowerCase();
                String value = values[1];
                map.put(name, value);
            }
        }
        return map;
    }

    private String decode(String value) {
        if (value == null) {
            return null;
        }
        try {
            return URLDecoder.decode(value, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            LOG.warn("invalid encoding", e);
            return value;
        }
    }

}
