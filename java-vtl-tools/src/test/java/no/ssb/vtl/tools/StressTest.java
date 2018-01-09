package no.ssb.vtl.tools;

/*
 * ========================LICENSE_START=================================
 * Java VTL
 * %%
 * Copyright (C) 2016 - 2017 Hadrien Kohl
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *      http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * =========================LICENSE_END==================================
 */

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import no.ssb.vtl.connectors.Connector;
import no.ssb.vtl.connectors.ConnectorException;
import no.ssb.vtl.connectors.spring.RestTemplateConnector;
import no.ssb.vtl.connectors.spring.converters.DataHttpConverter;
import no.ssb.vtl.connectors.spring.converters.DataStructureHttpConverter;
import no.ssb.vtl.connectors.spring.converters.DatasetHttpMessageConverter;
import no.ssb.vtl.connectors.utils.RegexConnector;
import no.ssb.vtl.model.Dataset;
import no.ssb.vtl.script.VTLScriptEngine;
import org.junit.Before;
import org.junit.Test;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.scheduling.concurrent.ConcurrentTaskExecutor;
import org.springframework.web.client.RestTemplate;

import javax.script.Bindings;
import javax.script.ScriptException;
import javax.script.SimpleBindings;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

public class StressTest {

    ConsoleReporter reporter;
    private MetricRegistry metrics;
    private Connector connector;
    private VTLScriptEngine engine;
    private Bindings bindings;

    @Before
    public void setUp() throws Exception {
        metrics = new MetricRegistry();
        reporter = ConsoleReporter.forRegistry(metrics)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build();
        reporter.start(5, TimeUnit.SECONDS);
        connector = getKompisConnector(new ObjectMapper());
        engine = new VTLScriptEngine();
        bindings = new SimpleBindings();
    }


    Connector getKompisConnector(ObjectMapper mapper) {

        SimpleClientHttpRequestFactory schrf = new SimpleClientHttpRequestFactory();
        schrf.setBufferRequestBody(false);
        schrf.setReadTimeout(1000);
        schrf.setConnectTimeout(1000);

        schrf.setTaskExecutor(new ConcurrentTaskExecutor());

        schrf.setConnectTimeout(200);
        schrf.setReadTimeout(1000);

        ExecutorService executorService = Executors.newCachedThreadPool();

        RestTemplate template = new RestTemplate(schrf);

        template.getMessageConverters().add(
                0, new DataHttpConverter(mapper)
        );
        template.getMessageConverters().add(
                0, new DataStructureHttpConverter(mapper)
        );
        template.getMessageConverters().add(
                0, new DatasetHttpMessageConverter(mapper)
        );

        RestTemplateConnector restTemplateConnector = new RestTemplateConnector(
                template,
                executorService
        );

        // TODO: Remove when old API is deprecated.
        return RegexConnector.create(restTemplateConnector,
                Pattern.compile("(?<host>(?:http|https)://.*?)/api/data/(?<id>.*)/latest(?<param>[?|#].*)"),
                "${host}/api/v3/data/${id}${param}"
        );
    }

    @Test
    public void testBigDataset() throws ConnectorException, ScriptException {


        Meter requests = metrics.meter("requests");
        Connector connector = getKompisConnector(new ObjectMapper());

        Dataset dataset = connector.getDataset("http://al-kostra-app-utv.ssb.no:7090/api/data/pG2Tk1nxRR2vU2Pp-tQvTg/latest?access_token=eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJlLXBvc3QiOiJzdXBlckBzc2Iubm8iLCJ1cG4iOiJzdXBlciIsInVzZXJfbmFtZSI6InN1cGVyIiwic2NvcGUiOlsicmVhZCIsIndyaXRlIl0sImlzcyI6Imh0dHA6Ly9hbC1rb3N0cmEtYXBwLXV0di5zc2Iubm86NzIwMCIsImV4cCI6MTgzMDg3NzkxOCwiYXV0aG9yaXRpZXMiOlsiUk9MRV9TVVBFUiJdLCJqdGkiOiI3NjRjMzg0NC1jODk1LTQ2ODMtYWQ2ZC02MmI2ODgxMmNjMDQiLCJjbGllbnRfaWQiOiJjb21tb25ndWkiLCJOYXZuIjoiU3VwZXIsIFN1cGVyIiwiR3J1cHBlciI6WyJLb21waXMgS29vcmRpbmF0b3IiLCJLb21waXMgQWRtaW5pc3RyYXRvciJdfQ.S6LL-VzMvMeEXMAVoK9JuDW8l1bV42l0V2FEukJDuC6VhUYEyXYlYn5CdRhWPfKZgMMqNyfnrndpqM3q5Xlsh9wIICH3KMH8A3TzyV8tGTCintjPSNE_fVUqqho499UBEQ5InSVskL-1dvADU4IorDYRiVnyJOA3SgXvPYcjZOvotYkU4eE4d1eqN0cGQwLZnnyOtm-yc7ae5HFmbBm3qyk4_esyCxg2iopr3T6wZ5tCsxB_uO42ITrV7umoUwCTHq3Rs4_2T8C7kavjpubZASV5wq7dDXgT-gzTv8yoGcHNhsBLh0cRu7kzzIRARPxNdOsS5J5gmZCB9ROS2joscg");


        dataset.getData().forEach(p -> requests.mark());
        reporter.report();

    }

    @Test
    public void bev_2016_fykomm_samlet() throws ScriptException, ConnectorException {
        Meter requests = metrics.meter("bev_2016_fykomm_samlet");
        Dataset dataset = connector.getDataset("http://al-kostra-app-utv.ssb.no:7090/api/data/sM-8zZZ1TqKYgzgdiZtcpA/latest?access_token=eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJlLXBvc3QiOiJzdXBlckBzc2Iubm8iLCJ1cG4iOiJzdXBlciIsInVzZXJfbmFtZSI6InN1cGVyIiwic2NvcGUiOlsicmVhZCIsIndyaXRlIl0sImlzcyI6Imh0dHA6Ly9hbC1rb3N0cmEtYXBwLXV0di5zc2Iubm86NzIwMCIsImV4cCI6MTgzMDg3NzkxOCwiYXV0aG9yaXRpZXMiOlsiUk9MRV9TVVBFUiJdLCJqdGkiOiI3NjRjMzg0NC1jODk1LTQ2ODMtYWQ2ZC02MmI2ODgxMmNjMDQiLCJjbGllbnRfaWQiOiJjb21tb25ndWkiLCJOYXZuIjoiU3VwZXIsIFN1cGVyIiwiR3J1cHBlciI6WyJLb21waXMgS29vcmRpbmF0b3IiLCJLb21waXMgQWRtaW5pc3RyYXRvciJdfQ.S6LL-VzMvMeEXMAVoK9JuDW8l1bV42l0V2FEukJDuC6VhUYEyXYlYn5CdRhWPfKZgMMqNyfnrndpqM3q5Xlsh9wIICH3KMH8A3TzyV8tGTCintjPSNE_fVUqqho499UBEQ5InSVskL-1dvADU4IorDYRiVnyJOA3SgXvPYcjZOvotYkU4eE4d1eqN0cGQwLZnnyOtm-yc7ae5HFmbBm3qyk4_esyCxg2iopr3T6wZ5tCsxB_uO42ITrV7umoUwCTHq3Rs4_2T8C7kavjpubZASV5wq7dDXgT-gzTv8yoGcHNhsBLh0cRu7kzzIRARPxNdOsS5J5gmZCB9ROS2joscg");
        bindings.put("ds1", dataset);
        engine.eval("" +
                "res := [ds1] {" +
                "  filter true" +
                "}" +
                "", bindings);

        Object res = bindings.get("res");
        if (res instanceof Dataset) {
            Dataset resultDataset = (Dataset) res;
            resultDataset.getData().forEach(p -> requests.mark());
        }
        reporter.report();
    }

    @Test
    public void testBigDataset2() throws ConnectorException, ScriptException {

        Meter requests = metrics.meter("requests");
        Dataset dataset = connector.getDataset("http://al-kostra-app-utv.ssb.no:7090/api/data/pG2Tk1nxRR2vU2Pp-tQvTg/latest?access_token=eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJlLXBvc3QiOiJzdXBlckBzc2Iubm8iLCJ1cG4iOiJzdXBlciIsInVzZXJfbmFtZSI6InN1cGVyIiwic2NvcGUiOlsicmVhZCIsIndyaXRlIl0sImlzcyI6Imh0dHA6Ly9hbC1rb3N0cmEtYXBwLXV0di5zc2Iubm86NzIwMCIsImV4cCI6MTgzMDg3NzkxOCwiYXV0aG9yaXRpZXMiOlsiUk9MRV9TVVBFUiJdLCJqdGkiOiI3NjRjMzg0NC1jODk1LTQ2ODMtYWQ2ZC02MmI2ODgxMmNjMDQiLCJjbGllbnRfaWQiOiJjb21tb25ndWkiLCJOYXZuIjoiU3VwZXIsIFN1cGVyIiwiR3J1cHBlciI6WyJLb21waXMgS29vcmRpbmF0b3IiLCJLb21waXMgQWRtaW5pc3RyYXRvciJdfQ.S6LL-VzMvMeEXMAVoK9JuDW8l1bV42l0V2FEukJDuC6VhUYEyXYlYn5CdRhWPfKZgMMqNyfnrndpqM3q5Xlsh9wIICH3KMH8A3TzyV8tGTCintjPSNE_fVUqqho499UBEQ5InSVskL-1dvADU4IorDYRiVnyJOA3SgXvPYcjZOvotYkU4eE4d1eqN0cGQwLZnnyOtm-yc7ae5HFmbBm3qyk4_esyCxg2iopr3T6wZ5tCsxB_uO42ITrV7umoUwCTHq3Rs4_2T8C7kavjpubZASV5wq7dDXgT-gzTv8yoGcHNhsBLh0cRu7kzzIRARPxNdOsS5J5gmZCB9ROS2joscg");
        bindings.put("ds1", dataset);
        engine.eval("" +
                "res := [ds1] {" +
                "  filter true" +
                "}" +
                "", bindings);

        Object res = bindings.get("res");
        if (res instanceof Dataset) {
            Dataset resultDataset = (Dataset) res;
            resultDataset.getData().forEach(p -> requests.mark());
        }
        reporter.report();

    }
}
