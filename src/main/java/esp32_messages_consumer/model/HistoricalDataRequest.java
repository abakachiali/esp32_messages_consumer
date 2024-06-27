package esp32_messages_consumer.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class HistoricalDataRequest {
    private List<String> fields;
    private String from;
    private String to;
}
