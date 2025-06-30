package com.bigtableproj;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import com.google.api.gax.rpc.NotFoundException;
import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.BulkMutation;
import com.google.cloud.bigtable.data.v2.models.Mutation;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import com.google.protobuf.ByteString;

public class BigTable {
    public final String projectId = "iitjdb-464002";     // use your GCP project ID
    public final String instanceId = "assignment4";      // use your Bigtable instance
    public BigtableDataClient dataClient;
    public BigtableTableAdminClient adminClient;
    public final String tableId = "weather";  // or whatever table name you're using
    public final String COLUMN_FAMILY = "sensor";  // Or whatever name you’re using
    public void connect() throws IOException {
    System.out.println("Connecting to Bigtable...");
    BigtableDataSettings dataSettings =
        BigtableDataSettings.newBuilder()
            .setProjectId(projectId)
            .setInstanceId(instanceId)
            .build();
    dataClient = BigtableDataClient.create(dataSettings);

    BigtableTableAdminSettings adminSettings =
        BigtableTableAdminSettings.newBuilder()
            .setProjectId(projectId)
            .setInstanceId(instanceId)
            .build();
    adminClient = BigtableTableAdminClient.create(adminSettings);
    
    System.out.println("Connected to Bigtable project: " + projectId + ", instance: " + instanceId);
}
public void close() {
    System.out.println("Closing Bigtable connections...");
    if (dataClient != null) {
        dataClient.close();
        System.out.println("Data client closed.");
    }
    if (adminClient != null) {
        adminClient.close();
        System.out.println("Admin client closed.");
    }
    System.out.println("All Bigtable clients closed.");
}

    public static void main(String[] args) {
    BigTable app = new BigTable();
    try {
        app.run(); // calls your custom flow
    } catch (Exception e) {
        System.err.println("Something went wrong: " + e.getMessage());
    }
}
public void run() throws Exception {
    connect();
    //deleteTable();
    //createTable();
    //loadData();
    //query1();
    //query2();
    //query3();
    //query4();
    query5();
    close();
}
public void deleteTable() {
    System.out.println("Cleaning up old table (if it exists): " + tableId);
    try {
        adminClient.deleteTable(tableId);
        System.out.println("Table '" + tableId + "' deleted successfully.");
    } catch (NotFoundException e) {
        System.out.println("Table '" + tableId + "' not found - nothing to delete. Skipping.");
    } catch (Exception e) {
        System.err.println("Failed to delete table '" + tableId + "': " + e.getMessage());
    }
}
public void loadData() throws Exception {
    String path = "src/bin/data/";
    try {
        System.out.println("Loading SeaTac");
        loadStationData(path + "seatac.csv", "SEA");

        System.out.println("Loading Vancouver");
        loadStationData(path + "vancouver.csv", "YVR");

        System.out.println("Loading Portland");
        loadStationData(path + "portland.csv", "PDX");
    } catch (Exception e) {
        throw new Exception("Load failed: " + e.getMessage(), e);
    }
}
public void loadStationData(String filename, String stationId) throws Exception {
    BufferedReader reader = new BufferedReader(new FileReader(filename));
    String line;
    int skipLines = 2;
    BulkMutation bulk = BulkMutation.create(tableId);

    while ((line = reader.readLine()) != null) {
        if (skipLines-- > 0) continue;

        String[] parts = line.split(",");
        if (parts.length < 9) continue;

        String date = parts[1].trim();      // YYYY-MM-DD
        String time = parts[2].trim();      // HH:mm
        String hour = time.split(":")[0];   // HH only
        String rowKey = stationId + "#" + date + "#" + hour;

        String temp = parts[3].trim();
        String dew  = parts[4].trim();
        String hum  = parts[5].trim();
        String wind = parts[6].trim();
        String gust = parts[7].trim();
        String pres = parts[8].trim();

        // Basic validation: skip if temperature is clearly corrupted
        if (!temp.matches("-?\\d+(\\.\\d+)?")) continue;

        Mutation m = Mutation.create()
            .setCell(COLUMN_FAMILY, "temperature", temp)
            .setCell(COLUMN_FAMILY, "dewPoint", dew)
            .setCell(COLUMN_FAMILY, "humidity", hum)
            .setCell(COLUMN_FAMILY, "windSpeed", wind)
            .setCell(COLUMN_FAMILY, "gust", gust)
            .setCell(COLUMN_FAMILY, "pressure", pres)
            .setCell(COLUMN_FAMILY, "time", time);

        bulk.add(rowKey, m);
    }

    reader.close();
    dataClient.bulkMutateRows(bulk);
    System.out.println("Done loading " + stationId);
}
public void query1() throws Exception {
    String rowKey = "YVR#2022-10-01#10";

    Row row = dataClient.readRow(tableId, rowKey);
    if (row == null) {
        System.out.println("No data found for: " + rowKey);
        return;
    }

    String temperature = "-";
    for (RowCell cell : row.getCells()) {
        if (cell.getFamily().equals(COLUMN_FAMILY)
            && cell.getQualifier().toStringUtf8().equals("temperature")) {
            
            temperature = cell.getValue().toStringUtf8().trim();
            break;
        }
    }

    System.out.println("Temperature at Vancouver (YVR) on 2022-10-01 at 10 a.m.: " + temperature + " °F");
}
/**
 * Query returns the highest wind speed in the month of September 2022 in Portland.
 */

public double query2() throws Exception {
    System.out.println("Executing query #2: Highest wind speed in Portland during September 2022.");
    double maxWindSpeed = Double.NEGATIVE_INFINITY;
    String maxRowKey = null;

    // Match MM-DD-YYYY key structure
    Query query = Query.create(tableId)
        .range(ByteStringRange.create("PDX#09-01-2022#00", "PDX#10-01-2022#00"));

    ServerStream<Row> rows = dataClient.readRows(query);

    for (Row row : rows) {
        for (RowCell cell : row.getCells(COLUMN_FAMILY, "windSpeed")) {
            String windSpeedStr = cell.getValue().toStringUtf8().trim();

            // Skip missing or invalid values
            if (!windSpeedStr.matches("-?\\d+(\\.\\d+)?")) continue;

            double windSpeed = Double.parseDouble(windSpeedStr);
            if (windSpeed > maxWindSpeed) {
                maxWindSpeed = windSpeed;
                maxRowKey = row.getKey().toStringUtf8();
            }
        }
    }

    if (maxWindSpeed == Double.NEGATIVE_INFINITY) {
        System.out.println("No valid wind speed data found for PDX in September 2022.");
        return 0.0;
    }

    System.out.println("Highest wind speed: " + maxWindSpeed + " mph");
    System.out.println("Found at: " + maxRowKey);
    return maxWindSpeed;
}
public void query3() throws Exception {
    // Updated: Broader key range to include all hourly rows (zero-padded or not)
    String startKey = "SEA#2022-10-02#";
    String endKey   = "SEA#2022-10-03#";

    Query query = Query.create(tableId)
        .range(ByteString.copyFromUtf8(startKey), ByteString.copyFromUtf8(endKey));

    System.out.println("SeaTac Readings on October 2, 2022\n");
    System.out.printf("%-6s %-12s %-12s %-12s %-12s %-12s\n",
        "Hour", "Temp (°F)", "Dewpoint", "Humidity", "Windspeed", "Pressure");
    System.out.println("--------------------------------------------------------------");

    List<Row> rowList = new ArrayList<>();
    dataClient.readRows(query).forEach(rowList::add);

    // Sort numerically by hour
    rowList.sort(Comparator.comparingInt(r -> {
        String[] parts = r.getKey().toStringUtf8().split("#");
        try {
            return (parts.length == 3) ? Integer.parseInt(parts[2]) : -1;
        } catch (NumberFormatException e) {
            return -1;
        }
    }));

    for (Row row : rowList) {
        String rowKey = row.getKey().toStringUtf8();
        String[] parts = rowKey.split("#");
        if (parts.length < 3) {
            System.err.println("Skipping malformed rowKey: " + rowKey);
            continue;
        }

        String hour = parts[2];
        String temp = "-", dew = "-", hum = "-", wind = "-", press = "-";

        for (RowCell cell : row.getCells()) {
            if (!cell.getFamily().equals(COLUMN_FAMILY)) continue;

            String col = cell.getQualifier().toStringUtf8();
            String val = cell.getValue().toStringUtf8().trim();
            String cleaned = val.matches("-?\\d+(\\.\\d+)?") ? val : "-";

            switch (col) {
                case "temperature": temp = cleaned; break;
                case "dewPoint":    dew = cleaned; break;
                case "humidity":    hum = cleaned; break;
                case "windSpeed":   wind = cleaned; break;
                case "pressure":    press = cleaned; break;
            }
        }

        System.out.printf("%-6s %-12s %-12s %-12s %-12s %-12s\n",
            hour, temp, dew, hum, wind, press);
    }
}
public double query4() throws Exception {
    System.out.println("Executing query #4: Highest temperature in July & August 2022.");
    double maxTemp = Double.NEGATIVE_INFINITY;
    String maxRowKey = null;

    // Scan both July and August ranges
    List<ByteStringRange> summerRanges = List.of(
        ByteStringRange.create("AAA#07-01-2022#00", "ZZZ#08-01-2022#00"), // July
        ByteStringRange.create("AAA#08-01-2022#00", "ZZZ#09-01-2022#00")  // August
    );

    for (ByteStringRange range : summerRanges) {
        Query query = Query.create(tableId).range(range);
        ServerStream<Row> rows = dataClient.readRows(query);

        for (Row row : rows) {
            for (RowCell cell : row.getCells(COLUMN_FAMILY, "temperature")) {
                String tempStr = cell.getValue().toStringUtf8().trim();
                if (!tempStr.matches("-?\\d+(\\.\\d+)?")) continue;

                double temp = Double.parseDouble(tempStr);
                if (temp > maxTemp) {
                    maxTemp = temp;
                    maxRowKey = row.getKey().toStringUtf8();
                }
            }
        }
    }

    if (maxTemp == Double.NEGATIVE_INFINITY) {
        System.out.println("No valid temperature data found for July or August 2022.");
        return 0.0;
    }

    System.out.println("Highest temperature: " + maxTemp + " °F");
    System.out.println("Found at: " + maxRowKey);
    return maxTemp;
}
public double query5() throws Exception {
    System.out.println("Executing query #5: Lowest pressure in December 2022.");
    double minPressure = Double.POSITIVE_INFINITY;
    String minRowKey = null;

    // Scan all stations for December 2022
    Query query = Query.create(tableId)
        .range(ByteStringRange.create("AAA#12-01-2022#00", "ZZZ#01-01-2023#00"));

    ServerStream<Row> rows = dataClient.readRows(query);

    for (Row row : rows) {
        for (RowCell cell : row.getCells(COLUMN_FAMILY, "pressure")) {
            String pressureStr = cell.getValue().toStringUtf8().trim();
            if (!pressureStr.matches("-?\\d+(\\.\\d+)?")) continue;

            double pressure = Double.parseDouble(pressureStr);
            if (pressure < minPressure) {
                minPressure = pressure;
                minRowKey = row.getKey().toStringUtf8();
            }
        }
    }

    if (minPressure == Double.POSITIVE_INFINITY) {
        System.out.println("No valid pressure data found for December 2022.");
        return 0.0;
    }

    System.out.println("Lowest pressure recorded: " + minPressure + " hPa");
    System.out.println("Found at: " + minRowKey);
    return minPressure;
}
public void createTable() {
    System.out.println("Creating table: " + tableId);
    try {
        // Create table with a single column family
        CreateTableRequest request = CreateTableRequest.of(tableId)
            .addFamily(COLUMN_FAMILY);

        adminClient.createTable(request);
        System.out.println("Table '" + tableId + "' created with column family '" + COLUMN_FAMILY + "'.");

        // Now print the schema
        System.out.println("Fetching schema details:");
        var table = adminClient.getTable(tableId);
        table.getColumnFamilies().forEach(cf -> {System.out.println("Column Family: " + cf.getId());System.out.println("   GC Rule: " + cf.getGCRule());});

    } catch (Exception e) {
        System.err.println("Error creating table '" + tableId + "': " + e.getMessage());
    }
}
}