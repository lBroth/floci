package io.floci.aws.services.kinesisanalytics;

import com.github.dockerjava.api.DockerClient;
import io.floci.aws.services.kinesisanalytics.model.FlinkApplication;
import io.floci.aws.testing.DockerAvailability;
import io.floci.aws.testing.RestAssuredJsonUtils;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import io.restassured.response.Response;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static io.restassured.RestAssured.given;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * #5664: a Managed Service for Apache Flink application comes back after Floci restarts. Shutdown removes the
 * application's Flink cluster; the startup restore starts a new one, so the application is RUNNING again and its
 * JobManager answers. Removing the JobManager stands in for the shutdown, and the restore is what startup calls.
 *
 * <p>A nightly check: it runs a real Flink JobManager, which the shared test configuration mocks.
 */
@QuarkusTest
@TestProfile(KinesisAnalyticsFlinkRestartNightlyIntegrationTest.Profile.class)
@EnabledIfSystemProperty(named = "nightly.checks.enabled", matches = "true")
// Bounded waits: the Flink image's pull, then the cluster starting twice.
@Timeout(value = 10, unit = TimeUnit.MINUTES)
class KinesisAnalyticsFlinkRestartNightlyIntegrationTest {

    /** Real Flink clusters. */
    public static final class Profile implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            return Map.of("floci.services.kinesis-analytics.mock", "false");
        }
    }

    private static final String AUTH = "AWS4-HMAC-SHA256 Credential=AKID/20260916/us-east-1/kinesisanalytics/"
            + "aws4_request, SignedHeaders=host, Signature=abc";
    private static final String CONTENT_TYPE = "application/x-amz-json-1.1";
    private static final Duration APPLICATION_TIMEOUT = Duration.ofMinutes(4);

    private final HttpClient http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1)
            .connectTimeout(Duration.ofSeconds(5)).build();

    @Inject
    DockerClient dockerClient;

    @Inject
    KinesisAnalyticsV2Service kinesisAnalyticsService;

    @BeforeAll
    static void configureRestAssured() {
        RestAssuredJsonUtils.configureAwsContentTypes();
    }

    @BeforeEach
    void requireDocker() {
        Assumptions.assumeTrue(DockerAvailability.answers(dockerClient),
                "Docker daemon must be available for the Flink restart check");
    }

    @Test
    void anApplicationIsRunningAgainAfterARestart() throws Exception {
        String name = "restart-" + UUID.randomUUID().toString().substring(0, 8);
        kda("CreateApplication", """
                {"ApplicationName": "%s", "RuntimeEnvironment": "FLINK-1_20",
                 "ServiceExecutionRole": "arn:aws:iam::000000000000:role/x"}
                """.formatted(name)).then().statusCode(200);
        try {
            kda("StartApplication", "{\"ApplicationName\": \"%s\", \"RunConfiguration\": {}}".formatted(name))
                    .then().statusCode(200);
            awaitRunning(name);
            FlinkApplication started = kinesisAnalyticsService.describeApplication(name, "us-east-1");
            String jobManager = started.getContainerId();
            assertEquals(200, overview(started.getRestEndpoint()));

            dockerClient.removeContainerCmd(jobManager).withForce(true).exec();
            kinesisAnalyticsService.restorePersistedRuntime().join();

            awaitRunning(name);
            FlinkApplication restored = kinesisAnalyticsService.describeApplication(name, "us-east-1");
            assertNotEquals(jobManager, restored.getContainerId());
            assertEquals(200, overview(restored.getRestEndpoint()));
        } finally {
            kda("StopApplication", "{\"ApplicationName\": \"%s\"}".formatted(name));
            long created = kinesisAnalyticsService.describeApplication(name, "us-east-1").getCreateTimestamp()
                    .getEpochSecond();
            kda("DeleteApplication", "{\"ApplicationName\": \"%s\", \"CreateTimestamp\": %d}".formatted(name, created));
        }
    }

    private void awaitRunning(String name) {
        await("application " + name + " running").atMost(APPLICATION_TIMEOUT).pollInterval(Duration.ofSeconds(2))
                .until(() -> kda("DescribeApplication", "{\"ApplicationName\": \"%s\"}".formatted(name))
                        .then().statusCode(200).extract().path("ApplicationDetail.ApplicationStatus"),
                        "RUNNING"::equals);
    }

    /** The status of the JobManager's {@code GET /overview}, which answers once the cluster is up. */
    private int overview(String restEndpoint) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(URI.create(restEndpoint + "/overview"))
                .timeout(Duration.ofSeconds(10)).GET().build();
        return http.send(request, HttpResponse.BodyHandlers.discarding()).statusCode();
    }

    private static Response kda(String action, String body) {
        return given().header("Authorization", AUTH)
                .header("X-Amz-Target", "KinesisAnalytics_20180523." + action).contentType(CONTENT_TYPE)
                .body(body).when().post("/");
    }
}
