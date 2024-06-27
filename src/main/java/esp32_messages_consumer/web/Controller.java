package esp32_messages_consumer.web;

import esp32_messages_consumer.model.HistoricalDataRequest;
import esp32_messages_consumer.service.DatasetsService;
import org.bson.Document;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.text.ParseException;
import java.util.List;
import java.util.logging.Logger;

@RestController
public class Controller {

    private final DatasetsService datasetsService;

    @Autowired
    public Controller(DatasetsService datasetsService) {
        this.datasetsService = datasetsService;
    }

    @PostMapping("/getHistoricalDatas")
    public List<Document> getHistoricalDatas(@RequestBody HistoricalDataRequest request) throws ParseException {
        request.getFields().add("date");
        return datasetsService.getHistoricalDatas(request);
    }
}
