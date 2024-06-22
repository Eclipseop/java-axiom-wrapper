package org.otterdev.axiom;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public final class Axiom {

  private static final int MAX_BATCH_SIZE = 1000;
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final HttpClient HTTP_CLIENT = HttpClient.newHttpClient();

  private final String apiToken;
  private final String datasetName;

  private final List<ObjectNode> requestBuffer = new ArrayList<>();

  private static Axiom instance;

  private Axiom(String apiToken, String datasetName) {
    this.apiToken = Objects.requireNonNull(apiToken);
    this.datasetName = Objects.requireNonNull(datasetName);
  }

  public synchronized static Axiom createInstance(String apiToken, String datasetName) {
    if (instance == null) {
      instance = new Axiom(apiToken, datasetName);
    }
    return instance;
  }

  public void ingest(String message) {
    ObjectNode node = MAPPER.createObjectNode();
    node.put("message", message);
    ingest(node, true);
  }

  public void ingest(ObjectNode node, boolean addTimestamp) {
    if (addTimestamp) {
      node.put("@timestamp", Instant.now().toString());
    }
    requestBuffer.add(node);
  }

  public void ingest(ObjectNode node) {
    ingest(node, true);
  }

  private void publish() throws IOException {
    List<ObjectNode> batchRequests;
    synchronized (requestBuffer) {
      int bufferSize = requestBuffer.size();
      int batchSize = Math.min(MAX_BATCH_SIZE, bufferSize);

      batchRequests = new ArrayList<>(requestBuffer.subList(0, batchSize));
      requestBuffer.subList(0, batchSize).clear();
    }

    if (batchRequests.isEmpty()) return;

    String requestBody = MAPPER.writeValueAsString(batchRequests);

    HttpRequest httpRequest = HttpRequest.newBuilder()
        .uri(URI.create(String.format("https://api.axiom.co/v1/datasets/%s/ingest", datasetName)))
        .header("Authorization", "Bearer " + apiToken)
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(requestBody))
        .build();

    HTTP_CLIENT.sendAsync(httpRequest, HttpResponse.BodyHandlers.discarding());
  }

  public void flush() throws IOException {
    publish();
  }
}
