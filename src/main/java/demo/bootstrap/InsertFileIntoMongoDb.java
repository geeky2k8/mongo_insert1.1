package demo.bootstrap;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.stream.Stream;
import java.util.zip.DataFormatException;

import org.apache.commons.lang3.StringUtils;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

import demo.mongo.dao.AbstractMongoDao;
import demo.utils.Config;
/**
 * This class used for convert the csv file into dbobject and stored in mongodb.
 * 
 * @author Kishore Kumar
 * @version 1.0
 */
public class InsertFileIntoMongoDb {
    
    private static Map<String, Map<String, String>> columnMapping = new LinkedHashMap<String,Map<String, String>>();

    private AbstractMongoDao mongoDao;

    public static void main(String[] args) {
        InsertFileIntoMongoDb bootstrap = new InsertFileIntoMongoDb();
        bootstrap.run();
    }

    private void run() {
        mongoDao = new AbstractMongoDao();

        try (Stream<String> stream = Files.lines(Paths.get(Config.mappingFile))) {
            stream.forEach(line -> {
                line = line.trim();
                if (StringUtils.isNotBlank(line)) {
                    Map<String, String> columnTypeMapping = new LinkedHashMap<>();
                    String[] lineSpliter = line.split("=");
                    String[] fileType = lineSpliter[0].split("\\.");
                    String fileName = fileType[0].trim();
                    String category = fileType[3].trim();
                    String recordLine = lineSpliter[1].trim();
                    switch (category) {
                    case "names":
                        readLine(recordLine, "\\|", category, columnTypeMapping);
                        columnMapping.put(fileName, columnTypeMapping);
                        break;
                    case "types":
                        readLine(recordLine, "\\|", category, columnMapping.get(fileName));
                        columnMapping.put(fileName, columnMapping.get(fileName));
                        break;
                    default:
                        break;
                    }
                }
            });
            for (Entry<String, Map<String, String>> ele : columnMapping.entrySet()) {
                readAndDumpDataFile(ele);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void readAndDumpDataFile(Entry<String, Map<String, String>> columnmap) {
        List<DBObject> dbObjects = new ArrayList<>();
        String filePath = Config.parentFolder + columnmap.getKey() + ".txt";
        try (Stream<String> stream = Files.lines(Paths.get(filePath))) {
            stream.forEach(line -> {
                try {
                    dbObjects.add(readDataLinee(line, "\\|", columnmap.getValue()));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });

            System.out.println("------------INSERTING DBOBJECTS INTO MONGODB FOR COLLECTION---------------");
            System.out.println("---------------------------"+columnmap.getKey()+"-------------------------");
            mongoDao.insert(columnmap.getKey(), dbObjects);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @SuppressWarnings("resource")
	private void readLine(String recordLine, String delim, String category,
            Map<String, String> columnTypeMapping) {
        Scanner lineScanner = new Scanner(recordLine);
        lineScanner.useDelimiter(delim);
        switch (category.trim()) {
        case "names":
            while (lineScanner.hasNext()) {
                columnTypeMapping.put(lineScanner.next(), null);
            }
            break;

        case "types":
            for (Entry<String, String> ele : columnTypeMapping.entrySet()) {
                String dataType = lineScanner.next();
                columnTypeMapping.put(ele.getKey(), "mcvisid".equals(dataType) ? "string" : dataType);
            }
            break;

        default:
            break;
        }

    }

    private BasicDBObject readDataLinee(String recordLine, String delim, Map<String, String> columnNames)
            throws DataFormatException {
        BasicDBObject dbObject = new BasicDBObject();
        String[] values = recordLine.split(delim);
        if (values == null || values.length != columnNames.size()) {
            throw new DataFormatException("Column and value length is not equal");
        }
        int _i = 0;
        for (Entry<String, String> ele : columnNames.entrySet()) {
            String value = values[_i];
            switch (ele.getValue()) {
            case "string":
                if (StringUtils.isNotBlank(value)) {
                    dbObject.put(ele.getKey(), value);
                }
                break;
            case "int":
                if (StringUtils.isNotBlank(value)) {
                    dbObject.put(ele.getKey(), Integer.valueOf(value));
                }
                break;
            default:
                break;
            }
            _i++;
        }
        return dbObject;
    }

}