package no.ssb.vtl.script.operations.hierarchy;

/*-
 * ========================LICENSE_START=================================
 * Java VTL
 * %%
 * Copyright (C) 2016 - 2017 Hadrien Kohl
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * =========================LICENSE_END==================================
 */

import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Streams;
import com.google.common.graph.GraphBuilder;
import com.google.common.graph.Graphs;
import com.google.common.graph.MutableGraph;
import com.google.common.graph.MutableValueGraph;
import com.google.common.graph.ValueGraphBuilder;
import com.google.common.io.Resources;
import no.ssb.vtl.model.Component;
import no.ssb.vtl.model.DataPoint;
import no.ssb.vtl.model.DataStructure;
import no.ssb.vtl.model.Dataset;
import no.ssb.vtl.model.Order;
import no.ssb.vtl.model.VTLObject;
import no.ssb.vtl.script.support.VTLPrintStream;
import org.brotli.dec.BrotliInputStream;
import org.junit.Test;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.Year;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.stream.Collectors.toList;
import static no.ssb.vtl.model.Component.Role.IDENTIFIER;
import static no.ssb.vtl.model.Component.Role.MEASURE;
import static no.ssb.vtl.script.operations.hierarchy.HierarchyOperation.findPaths;
import static no.ssb.vtl.script.operations.hierarchy.HierarchyOperation.sortTopologically;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

public class HierarchyOperationTest extends RandomizedTest {

    private static VTLPrintStream PS = new VTLPrintStream(System.out);
    private static ObjectMapper MAPPER = new ObjectMapper();

    private static Instant createInstant(int year) {
        return OffsetDateTime.of(
                year,
                1,
                1,
                0,
                0,
                0,
                0,
                ZoneOffset.UTC
        ).toInstant();
    }
    private static List<Object> createAggregatedPopulation() {
        return Lists.newArrayList(
                createInstant(2000), "Austria", 2000L, -2000L,
                createInstant(2000), "Belgium", 2000L, -2000L,
                createInstant(2000), "European Union", 10000L, -10000L,
                createInstant(2000), "Luxembourg", 2000L, -2000L,
                createInstant(2000), "Benelux", 6000L, -6000L,
                createInstant(2000), "Italy", 2000L, -2000L,
                createInstant(2000), "Holland", 2000L, -2000L,
                createInstant(2001), "Austria", 2001L, -2001L,
                createInstant(2001), "Belgium", 2001L, -2001L,
                createInstant(2001), "European Union", 10005L, -10005L,
                createInstant(2001), "Luxembourg", 2001L, -2001L,
                createInstant(2001), "Benelux", 6003L, -6003L,
                createInstant(2001), "Italy", 2001L, -2001L,
                createInstant(2001), "Holland", 2001L, -2001L,
                createInstant(2002), "Austria", 2002L, -2002L,
                createInstant(2002), "Belgium", 2002L, -2002L,
                createInstant(2002), "European Union", 10010L, -10010L,
                createInstant(2002), "Luxembourg", 2002L, -2002L,
                createInstant(2002), "Benelux", 6006L, -6006L,
                createInstant(2002), "Italy", 2002L, -2002L,
                createInstant(2002), "Holland", 2002L, -2002L,
                createInstant(2003), "Austria", 2003L, -2003L,
                createInstant(2003), "Belgium", 2003L, -2003L,
                createInstant(2003), "European Union", 10015L, -10015L,
                createInstant(2003), "Luxembourg", 2003L, -2003L,
                createInstant(2003), "Benelux", 6009L, -6009L,
                createInstant(2003), "Italy", 2003L, -2003L,
                createInstant(2003), "Holland", 2003L, -2003L
        );
    }

    private static Dataset create0ADataset(String file) {
        DataStructure structure = create0AStructure();
        return new Dataset() {
            @Override
            public Stream<DataPoint> getData() {
                return create0AData(structure, file);
            }

            @Override
            public Optional<Stream<DataPoint>> getData(Order orders, Filtering filtering, Set<String> components) {
                return Optional.of(getData());
            }

            @Override
            public Optional<Map<String, Integer>> getDistinctValuesCount() {
                return Optional.empty();
            }

            @Override
            public Optional<Long> getSize() {
                return Optional.empty();
            }

            @Override
            public DataStructure getDataStructure() {
                return structure;
            }
        };
    }

    private static DataStructure create0AStructure() {
        return DataStructure.builder()
                .put("PERIODE", IDENTIFIER, String.class)
                .put("ART_SEKTOR", IDENTIFIER, String.class)
                .put("BYDEL", IDENTIFIER, String.class)
                .put("FUNKSJON_KAPITTEL", IDENTIFIER, String.class)
                .put("KONTOKLASSE", IDENTIFIER, String.class)
                .put("REGION", IDENTIFIER, String.class)
                .put("BELOP", MEASURE, Long.class)
                .build();
    }

    private static Stream<DataPoint> create0AData(DataStructure structure, String file) {

        try {
            InputStream stream = openBroFile(file);

            JsonFactory factory = MAPPER.getFactory();
            JsonParser parser = factory.createParser(stream);

            parser.nextValue();
            parser.nextValue();
            MappingIterator<List<Object>> rows = MAPPER.readValues(parser, new TypeReference<List<Object>>() {
            });

            // Needed so that we can filter null ids.
            List<Component> ids = structure.values().stream()
                    .filter(Component::isIdentifier)
                    .collect(toList());

            return Streams.stream(rows).map(list -> {

                checkArgument(list.size() == structure.size());
                DataPoint dataPoint = DataPoint.create(structure.size());

                Iterator<Component> components = structure.values().iterator();
                for (int i = 0; i < list.size(); i++) {
                    VTLObject vtlObject = VTLObject.of(MAPPER.convertValue(list.get(i), components.next().getType()));
                    dataPoint.set(i, vtlObject);
                }
                return dataPoint;

            }).filter(dataPoint -> {
                Map<Component, VTLObject> asMap = structure.asMap(dataPoint);
                for (Component id : ids) {
                    if (asMap.get(id).get() == null) {
                        return false;
                    }
                }
                return true;
            });

        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }

    }

    private static InputStream openBroFile(String file) throws IOException {
        InputStream stream = Resources.getResource(HierarchyOperation.class, file).openStream();
        if (file.endsWith(".bro"))
            stream = new BrotliInputStream(stream);
        return stream;
    }

    private static Dataset createPopulationDataset() {
        DataStructure structure = DataStructure.builder()
                .put("Year", IDENTIFIER, Year.class)
                .put("Country", IDENTIFIER, String.class)
                .put("Population", MEASURE, Long.class)
                .put("OtherPopulation", MEASURE, Long.class)
                .build();

        List<Instant> years = Lists.newArrayList(
                Year.of(2000).atDay(1).atStartOfDay().toInstant(ZoneOffset.UTC),
                Year.of(2001).atDay(1).atStartOfDay().toInstant(ZoneOffset.UTC),
                Year.of(2002).atDay(1).atStartOfDay().toInstant(ZoneOffset.UTC),
                Year.of(2003).atDay(1).atStartOfDay().toInstant(ZoneOffset.UTC)
        );

        List<String> countries = Lists.newArrayList(
                "Austria",
                "Belgium",
                "Holland",
                "Italy",
                "Luxembourg"
        );

        ArrayList<DataPoint> data = Lists.newArrayList();
        for (Instant instant : years) {
            for (String country : countries) {
                int year = instant.atOffset(ZoneOffset.UTC).getYear();
                DataPoint point = structure.wrap(ImmutableMap.of(
                        "Year", instant,
                        "Country", country,
                        "Population", (long) year, //randomIntBetween(0, 20)
                        "OtherPopulation", (long) year * -1
                ));
                data.add(point);
            }
        }
        Collections.shuffle(data, new Random(randomLong()));

        return new Dataset() {
            @Override
            public Stream<DataPoint> getData() {
                return data.stream();
            }

            @Override
            public Optional<Map<String, Integer>> getDistinctValuesCount() {
                return Optional.empty();
            }

            @Override
            public Optional<Long> getSize() {
                return Optional.of((long) data.size());
            }

            @Override
            public Optional<Stream<DataPoint>> getData(Order orders, Filtering filtering, Set<String> components) {
                return Optional.of(getData().sorted(orders).filter(filtering).map(o -> {
                    // TODO
                    return o;
                }));
            }

            @Override
            public DataStructure getDataStructure() {
                return structure;
            }
        };
    }

    private static Dataset getAccountHierarchyDataset(String file) throws IOException {
        DataStructure hierarchyStructure = createAccountHierarchyStructure();
        List<DataPoint> hierarchyDataset = createAccountHierarchy(hierarchyStructure, file);
        return new Dataset() {

            @Override
            public Stream<DataPoint> getData() {
                return hierarchyDataset.stream();
            }

            @Override
            public Optional<Map<String, Integer>> getDistinctValuesCount() {
                return Optional.empty();
            }

            @Override
            public Optional<Long> getSize() {
                return Optional.empty();
            }

            @Override
            public Optional<Stream<DataPoint>> getData(Order orders, Filtering filtering, Set<String> components) {
                return Optional.of(getData().sorted(orders).filter(filtering).map(o -> {
                    // TODO
                    return o;
                }));
            }

            @Override
            public DataStructure getDataStructure() {
                return hierarchyStructure;
            }
        };
    }

    private static DataStructure createAccountHierarchyStructure() {
        return DataStructure.builder()
                .put("to", IDENTIFIER, String.class)
                .put("from", IDENTIFIER, String.class)
                .put("sign", MEASURE, String.class)
                .build();
    }

    private static List<DataPoint> createAccountHierarchy(DataStructure hierarchyStructure, String file) throws IOException {
        InputStream stream = openBroFile(file);

        JsonFactory factory = MAPPER.getFactory();
        JsonParser parser = factory.createParser(stream);

        parser.nextValue();
        parser.nextValue();
        MappingIterator<Map<String, Object>> rows = MAPPER.readValues(parser, new TypeReference<Map<String, Object>>() {
        });


        return Streams.stream(rows)
                .filter(map -> !map.get("from").toString().isEmpty())
                .filter(map -> !map.get("to").toString().isEmpty())
                .map((map) -> {
                    return hierarchyStructure.wrap(Maps.filterKeys(map, hierarchyStructure::containsKey));
                }).collect(toList());
    }

    private static Dataset createEmptyDataset(final DataStructure structure) {
        return new Dataset() {
            @Override
            public Stream<DataPoint> getData() {
                return Stream.empty();
            }

            @Override
            public Optional<Map<String, Integer>> getDistinctValuesCount() {
                return Optional.empty();
            }

            @Override
            public Optional<Long> getSize() {
                return Optional.empty();
            }

            @Override
            public Optional<Stream<DataPoint>> getData(Order orders, Filtering filtering, Set<String> components) {
                return Optional.of(getData().sorted(orders).filter(filtering).map(o -> {
                    // TODO
                    return o;
                }));
            }

            @Override
            public DataStructure getDataStructure() {
                return structure;
            }
        };
    }

    @Test
    public void testConstraintComponentIsInDataset() throws Exception {
        // Component must part of the structure.
        MutableValueGraph<VTLObject, Composition> graph = ValueGraphBuilder.directed().allowsSelfLoops(false).build();
        DataStructure structure = DataStructure.builder()
                .put("id1", IDENTIFIER, String.class)
                .put("id2", IDENTIFIER, String.class)
                .put("m1", MEASURE, Long.class)
                .build();

        DataStructure otherStructure = DataStructure.builder()
                .put("id1", IDENTIFIER, String.class)
                .put("id2", IDENTIFIER, String.class)
                .put("m1", MEASURE, Long.class)
                .build();

        Dataset dataset = createEmptyDataset(structure);

        assertThatThrownBy(() -> {
            new HierarchyOperation(dataset, graph, otherStructure.get("m1"));
        }).isNotNull().hasMessageContaining(otherStructure.get("m1").toString());
    }

    @Test
    public void testConstraintComponentIsNumeric() throws Exception {
        // Component must be an identifier.
        MutableValueGraph<VTLObject, Composition> graph = ValueGraphBuilder.directed().allowsSelfLoops(false).build();
        DataStructure structure = DataStructure.builder()
                .put("id1", IDENTIFIER, String.class)
                .put("id2", IDENTIFIER, String.class)
                .put("m1", MEASURE, Long.class)
                .build();

        Dataset dataset = createEmptyDataset(structure);

        assertThatThrownBy(() -> {
            new HierarchyOperation(dataset, graph, structure.get("m1"));
        }).isNotNull().hasMessageContaining("m1");
    }

    @Test
    public void testConstraintAllNumeric() {
        // All measures must be numeric
        MutableValueGraph<VTLObject, Composition> graph = ValueGraphBuilder.directed().allowsSelfLoops(false).build();
        DataStructure structure = DataStructure.builder()
                .put("id1", IDENTIFIER, String.class)
                .put("id2", IDENTIFIER, String.class)
                .put("m1", MEASURE, Long.class)
                .put("m2", MEASURE, Double.class)
                .put("m4", MEASURE, String.class)
                .build();

        Dataset dataset = createEmptyDataset(structure);

        assertThatThrownBy(() -> {
            new HierarchyOperation(dataset, graph, structure.get("id2"));
        }).isNotNull().hasMessageContaining("m4");
    }

    //@Test
    public void testConstaintGraphType() throws Exception {
        // TODO: The type of the graph should be the same as the component.
        fail("TODO");
    }

    @Test
    public void testTopologicalSort() throws Exception {
        MutableValueGraph<VTLObject, Composition> graph = ValueGraphBuilder.directed().allowsSelfLoops(false).build();
        graph.putEdgeValue(VTLObject.of("Austria"), VTLObject.of("European Union"), Composition.UNION);
        graph.putEdgeValue(VTLObject.of("Italy"), VTLObject.of("European Union"), Composition.UNION);
        graph.putEdgeValue(VTLObject.of("Holland"), VTLObject.of("European Union"), Composition.UNION);
        graph.putEdgeValue(VTLObject.of("Belgium"), VTLObject.of("European Union"), Composition.UNION);
        graph.putEdgeValue(VTLObject.of("Luxembourg"), VTLObject.of("European Union"), Composition.UNION);

        graph.putEdgeValue(VTLObject.of("European Union"), VTLObject.of("Benelux"), Composition.UNION);
        graph.putEdgeValue(VTLObject.of("Austria"), VTLObject.of("Benelux"), Composition.COMPLEMENT);
        graph.putEdgeValue(VTLObject.of("Italy"), VTLObject.of("Benelux"), Composition.COMPLEMENT);

        List<VTLObject> sort = sortTopologically(graph);

        assertThat(sort).extracting(VTLObject::get)
                .containsExactly(
                        "Austria",
                        "Italy",
                        "Holland",
                        "Belgium",
                        "Luxembourg",
                        "European Union",
                        "Benelux"
                );

    }

    @Test
    public void testCheckNoPath() throws Exception {

        MutableGraph<String> graph = GraphBuilder.directed().allowsSelfLoops(false).build();

        graph.putEdge("a", "b1");
        graph.putEdge("a", "b2");
        graph.putEdge("a", "b3");
        graph.putEdge("b1", "c1");
        graph.putEdge("b1", "c2");
        graph.putEdge("b1", "c3");

        graph.putEdge("b2", "d1");
        graph.putEdge("b2", "d2");
        graph.putEdge("b2", "d3");

        graph.putEdge("b3", "e1");
        graph.putEdge("b3", "e2");
        graph.putEdge("b3", "e3");

        assertThat(findPaths(graph, "e1", "a")).isEmpty();
        final int loops = 100000;

        Stopwatch findPathWatch = Stopwatch.createUnstarted();
        for (int i = 0; i < loops; i++) {
            findPathWatch.start();
            findPaths(graph, "e1", "a");
            findPathWatch.stop();
        }
        System.out.println("findPaths(): " + findPathWatch.elapsed(TimeUnit.NANOSECONDS) / loops);

        //assertThat(findPaths(graph, "a", "e1")).isNotEmpty();

        graph.putEdge("a", "e2");
        assertThat(Graphs.hasCycle(graph)).isFalse();
        assertThat(findPaths(graph, "e2", "a")).isEmpty();

        graph.putEdge("e1", "a");
        assertThat(Graphs.hasCycle(graph)).isTrue();
        assertThat(findPaths(graph, "e1", "a")).isNotEmpty();

        Stopwatch hasCyclePathWatch = Stopwatch.createUnstarted();
        for (int i = 0; i < loops; i++) {
            hasCyclePathWatch.start();
            Graphs.hasCycle(graph);
            hasCyclePathWatch.stop();
        }
        System.out.println("Graphs.hasCycle(): " + hasCyclePathWatch.elapsed(TimeUnit.NANOSECONDS) / loops);

//        Hadrien: disabled because it is not predictable.
//        assertThat(hasCyclePathWatch.elapsed(TimeUnit.NANOSECONDS) / loops)
//                .describedAs("time per guava Graph.hasCycle() call")
//                .isGreaterThan(findPathWatch.elapsed(TimeUnit.NANOSECONDS) / loops);

    }

    //@Test
    public void testIntegration2016() throws IOException {
        // See generate-data.sh

        Dataset testDataset = create0ADataset("kostra0a_grunnskole_driftregnskap_2016.json.bro");

        Dataset hierarchy = getAccountHierarchyDataset("account_hierarchy.json.bro");

        DataStructure dataStructure = testDataset.getDataStructure();
        PS.println(dataStructure);
        PS.println(hierarchy.getDataStructure());

        Component value = dataStructure.get("ART_SEKTOR");

        HierarchyOperation result = new HierarchyOperation(testDataset, hierarchy, value);

        PrintStream file = new PrintStream(new FileOutputStream("/Users/hadrien/Projects/java-vtl/result_account_2016"));
        result.getData().forEach(file::println);

    }

    //@Test
    public void testIntegration2015() throws IOException {
        // See generate-data.sh

        Dataset testDataset = create0ADataset("kostra0a_grunnskole_driftregnskap_2015.json.bro");
        Dataset hierarchy = getAccountHierarchyDataset("account_hierarchy.json.bro");

        DataStructure dataStructure = testDataset.getDataStructure();
        PS.println(dataStructure);
        PS.println(hierarchy.getDataStructure());

        Component value = dataStructure.get("ART_SEKTOR");
        HierarchyOperation result = new HierarchyOperation(testDataset, hierarchy, value);

        //PS.println(result);

        PrintStream file = new PrintStream(new FileOutputStream("/Users/hadrien/Projects/java-vtl/result_account_2015"));
        result.getData().forEach(file::println);

    }

    @Test
    public void testWithComplement() throws Exception {

        // Here the hierarchy uses complement
        //
        // Austria         -(+)-> European Union
        // ...             -(+)-> European Union
        //
        // European Union  -(+)-> Luxembourg
        //
        // Luxembourg      -(-)-> Benelux
        // Austria         -(-)-> Benelux
        // Italy         -(-)-> Benelux

        MutableValueGraph<VTLObject, Composition> graph = ValueGraphBuilder.directed().allowsSelfLoops(false).build();
        graph.putEdgeValue(VTLObject.of("Austria"), VTLObject.of("European Union"), Composition.UNION);
        graph.putEdgeValue(VTLObject.of("Italy"), VTLObject.of("European Union"), Composition.UNION);
        graph.putEdgeValue(VTLObject.of("Holland"), VTLObject.of("European Union"), Composition.UNION);
        graph.putEdgeValue(VTLObject.of("Belgium"), VTLObject.of("European Union"), Composition.UNION);
        graph.putEdgeValue(VTLObject.of("Luxembourg"), VTLObject.of("European Union"), Composition.UNION);

        graph.putEdgeValue(VTLObject.of("European Union"), VTLObject.of("Benelux"), Composition.UNION);
        graph.putEdgeValue(VTLObject.of("Austria"), VTLObject.of("Benelux"), Composition.COMPLEMENT);
        graph.putEdgeValue(VTLObject.of("Italy"), VTLObject.of("Benelux"), Composition.COMPLEMENT);

        // Year, Country, Pop
        Dataset population = createPopulationDataset();

        PS.println(population);

        // Country is the identifier we operate on.
        DataStructure structure = population.getDataStructure();
        Component value = structure.get("Country");
        HierarchyOperation result = new HierarchyOperation(population, graph, value);

        PS.println(result);

        List<Object> aggregatedPopulation = createAggregatedPopulation();

        assertThat(result.getData())
                .flatExtracting(input -> input)
                .extracting(VTLObject::get)
                .containsOnlyElementsOf(aggregatedPopulation);

    }

    @Test
    public void testWithComposition() throws Exception {

        // Here the hierarchy contains two levels;
        //
        // Luxembourg -> Benelux
        // ... -> Benelux
        //
        // Benelux -> European Union
        // Austria -> European Union
        // ... -> European Union

        MutableValueGraph<VTLObject, Composition> graph = ValueGraphBuilder.directed().allowsSelfLoops(false).build();
        graph.putEdgeValue(VTLObject.of("Austria"), VTLObject.of("European Union"), Composition.UNION);
        graph.putEdgeValue(VTLObject.of("Italy"), VTLObject.of("European Union"), Composition.UNION);
        graph.putEdgeValue(VTLObject.of("Benelux"), VTLObject.of("European Union"), Composition.UNION);
        graph.putEdgeValue(VTLObject.of("Holland"), VTLObject.of("Benelux"), Composition.UNION);
        graph.putEdgeValue(VTLObject.of("Belgium"), VTLObject.of("Benelux"), Composition.UNION);
        graph.putEdgeValue(VTLObject.of("Luxembourg"), VTLObject.of("Benelux"), Composition.UNION);

        // Year, Country, Pop
        Dataset population = createPopulationDataset();

        PS.println(population);

        // Country is the identifier we operate on.
        // Population is the value we operate on.
        DataStructure structure = population.getDataStructure();
        Component value = structure.get("Country");
        HierarchyOperation result = new HierarchyOperation(population, graph, value);

        PS.println(result);

        List<Object> aggregatedPopulation = createAggregatedPopulation();

        assertThat(result.getData())
                .flatExtracting(input -> input)
                .extracting(VTLObject::get)
                .containsOnlyElementsOf(aggregatedPopulation);

    }

    @Test
    public void testWithoutComposition() throws Exception {

        // Here the hierarchy contains node that are connected to two others nodes;
        // Luxembourg -> European Union
        // Benelux -> European Union

        MutableValueGraph<VTLObject, Composition> graph = ValueGraphBuilder.directed().allowsSelfLoops(false).build();
        graph.putEdgeValue(VTLObject.of("Austria"), VTLObject.of("European Union"), Composition.UNION);
        graph.putEdgeValue(VTLObject.of("Italy"), VTLObject.of("European Union"), Composition.UNION);
        graph.putEdgeValue(VTLObject.of("Holland"), VTLObject.of("European Union"), Composition.UNION);
        graph.putEdgeValue(VTLObject.of("Belgium"), VTLObject.of("European Union"), Composition.UNION);
        graph.putEdgeValue(VTLObject.of("Luxembourg"), VTLObject.of("European Union"), Composition.UNION);
        graph.putEdgeValue(VTLObject.of("Holland"), VTLObject.of("Benelux"), Composition.UNION);
        graph.putEdgeValue(VTLObject.of("Belgium"), VTLObject.of("Benelux"), Composition.UNION);
        graph.putEdgeValue(VTLObject.of("Luxembourg"), VTLObject.of("Benelux"), Composition.UNION);

        // Year, Country, Pop
        Dataset population = createPopulationDataset();

        PS.println(population);

        // Country is the identifier we operate on.
        // Population is the value we operate on.
        DataStructure structure = population.getDataStructure();
        Component value = structure.get("Country");
        HierarchyOperation result = new HierarchyOperation(population, graph, value);

        PS.println(result);

        List<Object> aggregatedPopulation = createAggregatedPopulation();

        assertThat(result.getData())
                .flatExtracting(input -> input)
                .extracting(VTLObject::get)
                .containsOnlyElementsOf(aggregatedPopulation);

    }
}
