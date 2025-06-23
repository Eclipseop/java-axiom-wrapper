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
import java.util.concurrent.*;

public final class Axiom {

  private static final int MAX_BATCH_SIZE = 1000;
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final HttpClient HTTP_CLIENT = HttpClient.newHttpClient();

  private final String apiToken;
  private final String datasetName;
  private ScheduledExecutorService executorService;

  private final BlockingQueue<ObjectNode> requestQueue = new LinkedBlockingQueue<>();

  private Axiom(String apiToken, String datasetName, boolean enableExecutor) {
    this.apiToken = Objects.requireNonNull(apiToken);
    this.datasetName = Objects.requireNonNull(datasetName);

    if (enableExecutor) {
      executorService = Executors.newSingleThreadScheduledExecutor();
      executorService.scheduleAtFixedRate(
          () -> {
            try {
              publish();
            } catch (IOException | InterruptedException e) {
              throw new RuntimeException(e);
            }
          },
          0,
          1,
          TimeUnit.SECONDS);
    }
  }

  public static class AxiomBuilder {
    private String apiToken;
    private String datasetName;
    private boolean enableExecutor;

    public static AxiomBuilder newBuilder() {
      return new AxiomBuilder();
    }

    /**
     * Sets the Axiom API Token
     * 
     * @param apiToken API Token provided by Axiom
     * @return this builder
     */
    public AxiomBuilder setApiToken(String apiToken) {
      this.apiToken = apiToken;
      return this;
    }

    /**
     * Sets the Axiom dataset name
     * 
     * @param datasetName Axiom dataset name to ingest into
     * @return this builder
     */
    public AxiomBuilder setDatasetName(String datasetName) {
      this.datasetName = datasetName;
      return this;
    }

    /**
     * Creates a {@link ScheduledExecutorService} to publish the events every 1
     * second.
     * This is advised unless needing fine grain control over publishing
     * 
     * @return this builder
     */
    public AxiomBuilder enableExecutor() {
      this.enableExecutor = true;
      return this;
    }

    /**
     * Builds and returns {@link Axiom}
     * 
     * @return A new Axiom Object
     */
    public Axiom build() {
      return new Axiom(apiToken, datasetName, enableExecutor);
    }
  }

  /**
   * Ingest a simple message. Will populate in Axiom with the format
   * message:your_message
   * 
   * @param message Simple message to ingest
   */
  public void ingest(String message) {
    ObjectNode node = MAPPER.createObjectNode();
    node.put("message", message);
    ingest(node);
  }

  /**
   * Ingest an {@link Object}. Will attempt to serialize as a JSON object.
   * 
   * @param object Java Object to ingest
   */
  public void ingest(Object object) {
    ingest(MAPPER.valueToTree(object));
  }

  private void ingest(ObjectNode node) {
    node.put("@timestamp", Instant.now().toString());
    requestQueue.add(node);
  }

  private void publish() throws IOException, InterruptedException {
    List<ObjectNode> batchRequests = new ArrayList<>();
    requestQueue.drainTo(batchRequests, MAX_BATCH_SIZE);

    if (batchRequests.isEmpty()) {
      return;
    }

    String requestBody = MAPPER.writeValueAsString(batchRequests);

    HttpRequest httpRequest = HttpRequest.newBuilder()
        .uri(
            URI.create(
                String.format("https://api.axiom.co/v1/datasets/%s/ingest", datasetName)))
        .header("Authorization", "Bearer " + apiToken)
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(requestBody))
        .build();

    HTTP_CLIENT.send(httpRequest, HttpResponse.BodyHandlers.discarding());
  }

  /**
   * Publishes submitted events to Axiom. Calling this is not required if
   * {@link AxiomBuilder#enableExecutor()} is set.
   */
  public void flush() {
    try {
      publish();
    } catch (IOException | InterruptedException e) {
      throw new RuntimeException("Failed to flush Axiom events", e);
    }
  }

  /**
   * Attempts to flush the remaining queued events. Shuts down the
   * {@link ScheduledExecutorService} if created via
   * {@link AxiomBuilder#enableExecutor()}
   */
  public void shutdown() {
    // ensure there is no pending events
    for (int i = 0; i < 10; i++) {
      if (requestQueue.isEmpty()) {
        break;
      }
      flush();
    }
    if (executorService != null) {
      executorService.shutdown();
    }
  }
}
