package esp32_messages_consumer.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import esp32_messages_consumer.model.HistoricalDataRequest;
import org.apache.logging.log4j.LogManager;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.lang.reflect.Array;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Service
public class DatasetsService implements DatasetsInterface {

    @Value("${mongodb.database.collection}")
    String dbCollection;

    @Value("${spring.data.mongodb.database}")
    String dbName;
    private static org.apache.logging.log4j.Logger logger = LogManager.getLogger(DatasetsService.class);

    DateFormat dateField_dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    @Value("${blg.data.document.valueDate.fieldName}")
    String valueDateFieldName;
    @Value("${blg.request.startTime.fieldName}")
    String requestStartTimeFieldName;
    @Value("${blg.data.document.validate}")
    List<String> blgrequiredFieldNames;
    private final MongoClient mongoClient;
    ObjectMapper mapper = new ObjectMapper();
    //@Autowired
    private final MongoTemplate mongoTemplate;

    @Autowired
    public DatasetsService(MongoClient mongoClient, MongoTemplate mongoTemplate) {
        this.mongoClient = mongoClient;
        this.mongoTemplate = mongoTemplate;
    }

    public static boolean isFileContentJSON(String fileContent) {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.readTree(fileContent);
            return true;
        } catch (IOException e) {

            return false;
        }
    }
    public int insertInto(String collection, List<Document> mongoDocs) {
        try {
            if (!mongoDocs.isEmpty()) {
                Collection<Document> inserts = mongoTemplate.insert(mongoDocs, collection);
                return inserts.size();
            }
        } catch (DataIntegrityViolationException e) {
            logger.error("DataIntegrityViolationException {}", e.getMessage());

            if (e.getCause() instanceof MongoBulkWriteException) {
                return ((MongoBulkWriteException) e.getCause())
                        .getWriteResult()
                        .getInsertedCount();
            }
            return 0;
        }
        return 0;
    }

    @Override
    public List<Document> getHistoricalDatas(HistoricalDataRequest request) throws ParseException {
        logger.info(request.toString());
        MongoDatabase database = mongoClient.getDatabase(dbName);
        MongoCollection<Document> collection = database.getCollection(dbCollection);

        List<Document> results = new ArrayList<>();

        try (MongoCursor<Document> cursor = collection.find().iterator()) {
            while (cursor.hasNext()) {
                results.add(cursor.next());
            }
        }
        DateFormat formatter = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
        formatter.setTimeZone(TimeZone.getTimeZone("UTC+1"));
        Date startDate = formatter.parse(request.getFrom()+" 00:00:00");
        Date endDate = formatter.parse(request.getTo()+" 23:59:59");
        Document filterD = null;
        //System.out.println(formatter.format(endDate));
        Document filterHist = null;
        // Filtre sur le champ date; Uniquement pour les requetes histories
        filterHist = new Document("FORMATED_"+requestStartTimeFieldName, new Document("$exists", true));
        filterD = new Document("FORMATED_"+requestStartTimeFieldName, new Document("$gte", startDate)
                .append("$lte", endDate));
        Document filter = new Document("$and",List.of(filterD, filterHist));
        Document projection = Document.parse(Projections.include(request.getFields()).toBsonDocument().toJson());
        results = collection
                .find(filter)
                .projection(projection)
                .into(new ArrayList<>());
        logger.info("count {}", results.size());
        return results;
    }

    @Override
    public String processFileDataContent(Object m) throws IOException, ParseException {
        if (doesCollectionExists(dbCollection)) {
            System.out.println("data " + m);
            List<Document> docs = generateMongoDocs(List.of(m.toString()));
            int count = insertInto(dbCollection, docs);
            return count+"";
        } else {
            logger.info("data {}", m);
            logger.error("--------> Erreur lors de la connexion à la collection. La collection");
            return "Erreur lors de la connexion à la collection. La collection";
        }
    }

    @Override
    public String importTo(String collection, List<String> jsonLines) throws ParseException {
        List<Document> mongoDocs = generateMongoDocs(jsonLines);
        int inserts = insertInto(collection, mongoDocs);
        return inserts + "/" + jsonLines.size();
    }

    public List<Document> lines(String json) {

        Boolean isAJsonFile = isFileContentJSON(json);
        if (!isAJsonFile) {
            logger.error("Le fichier : {}\n" +
                    "en instance n'est pas au format json");
            return null;
        }

        List<Document> documents = new ArrayList<>();
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode root = objectMapper.readTree(json);
            for (JsonNode jsonNode : root) {
                String document = objectMapper.writeValueAsString(jsonNode);
                Document documentOfFile = Document.parse(document);
                documents.add(documentOfFile);
            }
        } catch (IOException e) {
            return null;
        }
        return documents;

    }

    public Boolean doesCollectionExists(String collectionName) {
        return mongoTemplate.collectionExists(collectionName);
    }

    private List<Document> generateMongoDocs(List<String> lines) throws ParseException {
        List<Document> tempDocs = new ArrayList<>();
        List<Document> docs = new ArrayList<>();
        for (String json : lines) {
            tempDocs.add(Document.parse(json));
        }

        for (Document json : tempDocs) {
            if(json.containsKey("date")) {
                logger.info(json.get("date").toString());
                DateFormat dateField_dateFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
                TimeZone time_zone
                        = TimeZone.getTimeZone("GMT+2");
                dateField_dateFormat.setTimeZone(time_zone);
                Date date = dateField_dateFormat.parse(json.get("date").toString());
                json.put("FORMATED_"+requestStartTimeFieldName, date);
                docs.add(json);
            }
        }

        return docs;
    }

    public boolean isDuplicate(Document document) {
        Query query = new Query();
        for (String key : document.keySet()) {
            Object value = document.get(key);
            query.addCriteria(Criteria.where(key).is(value));
        }
        return mongoTemplate.exists(query, dbCollection);
    }

    private Boolean isvalidDocument(Document document, List<String> keys) {

        if (document.isEmpty())
            return false;
        else {
            for (String key : keys) {
                System.out.println("**********" + key + "----------" + document);
                if (!document.containsKey(key)) {
                    logger.warn("Le document \n{} \n ne contient pas de valeur {}. Il semblerait qu'il n'est pas un document valide", document.toJson(), key);
                    return false;
                }
            }
            return true;

        }
    }
}
