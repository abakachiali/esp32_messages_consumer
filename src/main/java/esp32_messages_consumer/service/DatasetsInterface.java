package esp32_messages_consumer.service;

import esp32_messages_consumer.model.HistoricalDataRequest;
import org.bson.Document;

import java.io.IOException;
import java.text.ParseException;
import java.util.List;

public interface DatasetsInterface {
	String processFileDataContent(Object m) throws IOException, ParseException;
	List<Document> getHistoricalDatas(HistoricalDataRequest request) throws ParseException;
	public String importTo(String collection, List<String> jsonLines) throws ParseException;
}
