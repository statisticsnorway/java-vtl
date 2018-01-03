package no.ssb.vtl.script.visitors;

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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import no.ssb.vtl.model.Component;
import no.ssb.vtl.model.DataPoint;
import no.ssb.vtl.model.DataStructure;
import no.ssb.vtl.model.Dataset;
import no.ssb.vtl.model.StaticDataset;
import no.ssb.vtl.model.VTLObject;
import no.ssb.vtl.parser.VTLLexer;
import no.ssb.vtl.parser.VTLParser;
import no.ssb.vtl.script.error.ContextualRuntimeException;
import no.ssb.vtl.script.functions.AggregationAvgFunction;
import no.ssb.vtl.script.functions.AggregationSumFunction;
import no.ssb.vtl.script.operations.AggregationOperation;
import org.antlr.v4.runtime.BailErrorStrategy;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.assertj.core.api.JUnitSoftAssertions;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import javax.script.SimpleBindings;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.*;

public class AggregationVisitorTest {

    private AggregationVisitor visitor;
    private Dataset datasetSingleMeasure;
    private Dataset datasetMultiMeasure;
    private Dataset datasetMultiMeasureWithNullValues;
    private DataStructure dataStructureSingleMeasure;

    @Rule
    public JUnitSoftAssertions softly = new JUnitSoftAssertions();

    @Before
    public void setUp() {

        SimpleBindings bindings = new SimpleBindings();
        visitor = new AggregationVisitor(
                new DatasetExpressionVisitor(
                        new ExpressionVisitor(
                                bindings
                        )
                )
        );

        dataStructureSingleMeasure = DataStructure.of(
                "time", Component.Role.IDENTIFIER, String.class,
                "geo", Component.Role.IDENTIFIER, String.class,
                "m1", Component.Role.MEASURE, Long.class);

        datasetSingleMeasure = StaticDataset.create(dataStructureSingleMeasure)
                .addPoints("2010", "NO", 20L)
                .addPoints("2011", "SE", 31L)
                .addPoints("2012", "SE", 41L)
                .addPoints("2012", "NO", 72L)
                .addPoints("2010", "SE", 40L)
                .addPoints("2011", "NO", 11L)
                .addPoints("2012", "SE", 41L)
                .addPoints("2010", "DK", 60L)
                .addPoints("2011", "DK", 51L)
                .addPoints("2012", "DK", 92L)
                .build();

        DataStructure dataStructureMultiMeasure = DataStructure.of(
                "time", Component.Role.IDENTIFIER, String.class,
                "geo", Component.Role.IDENTIFIER, String.class,
                "m1", Component.Role.MEASURE, Long.class,
                "m2", Component.Role.MEASURE, Long.class);

        datasetMultiMeasure = StaticDataset.create(dataStructureMultiMeasure)
                .addPoints("2010", "NO", 20L, 2L)
                .addPoints("2010", "SE", 40L, 4L)
                .addPoints("2010", "DK", 60L, 6L)
                .addPoints("2011", "NO", 11L, 1L)
                .addPoints("2011", "SE", 31L, 3L)
                .addPoints("2011", "DK", 51L, 5L)
                .addPoints("2012", "NO", 72L, 7L)
                .addPoints("2012", "SE", 82L, 8L)
                .addPoints("2012", "DK", 92L, 9L)
                .build();



        DataStructure dataStructureMultiMeasureWithNullValues = DataStructure.of(
                "time", Component.Role.IDENTIFIER, String.class,
                "geo", Component.Role.IDENTIFIER, String.class,
                "m1", Component.Role.MEASURE, Long.class,
                "m2", Component.Role.MEASURE, Long.class);

        datasetMultiMeasureWithNullValues = StaticDataset.create(dataStructureMultiMeasureWithNullValues)
                .addPoints("2010", "NO", 20L, 2L)
                .addPoints("2010", "SE", 40L, 4L)
                .addPoints("2010", "DK", null, 6L)
                .addPoints("2011", "NO", 11L, 1L)
                .addPoints("2011", "SE", 31L, 3L)
                .addPoints("2011", "DK", 51L, 5L)
                .addPoints("2012", "NO", 72L, 7L)
                .addPoints("2012", "SE", null, 8L)
                .addPoints("2012", "DK", null, 9L)
                .build();

        bindings.put("dsMulti", datasetMultiMeasure);
        bindings.put("dsMultiWithNulls", datasetMultiMeasureWithNullValues);
        bindings.put("dsSingle", datasetSingleMeasure);

    }

    @Test
    public void variableDifferentDataset() {

        List<String> expressions = Arrays.asList(
                "sum(dsSingle) group by dsMulti.time",
                "sum(dsSingle) along dsMulti.time"
        );
        for (String expression : expressions) {

            softly.assertThatThrownBy(() -> visitor.visit(parse(expression)))
                    .isInstanceOf(ContextualRuntimeException.class)
                    .hasMessage("variable dsMulti.time does not belong to dataset dsSingle");
        }
    }

    @Test
    public void variableNotFound() {

        List<String> expressions = Arrays.asList(
                "sum(notFound) group by time",
                "sum(dsSingle) group by notFound",
                "sum(dsSingle) group by dsSingle.notFound",
                "sum(dsSingle.notFound) group by time",
                "sum(notFound) along geo",
                "sum(dsSingle) along notFound",
                "sum(dsSingle) along dsSingle.notFound",
                "sum(dsSingle.notFound) along time"
        );
        for (String expression : expressions) {

            softly.assertThatThrownBy(() -> visitor.visit(parse(expression)))
                    .isInstanceOf(ContextualRuntimeException.class)
                    .hasMessage("undefined variable notFound");
        }
    }

    private VTLParser.AggregationFunctionContext parse(String expression) {
        VTLLexer lexer = new VTLLexer(CharStreams.fromString(expression));
        VTLParser parser = new VTLParser(new CommonTokenStream(lexer));
        parser.setErrorHandler(new BailErrorStrategy());
        return parser.aggregationFunction();
    }

    @Test
    public void testAggregateOnMeasure() {

        List<String> expressions = Arrays.asList(
                "sum(dsMulti) group by m2",
                "sum(dsMulti) group by dsMulti.m2",
                "sum(dsMulti.m1) group by m2",
                "sum(dsMulti.m1) group by dsMulti.m2",
                "sum(dsMulti) along m2",
                "sum(dsMulti) along dsMulti.m2",
                "sum(dsMulti.m1) along m2",
                "sum(dsMulti.m1) along dsMulti.m2"
        );
        for (String expression : expressions) {

            softly.assertThatThrownBy(() -> visitor.visit(parse(expression)))
                    .isInstanceOf(ContextualRuntimeException.class)
                    .hasMessage("variable m2 was not an identifier");

        }
    }

    @Test
    public void testMembership() {

        List<String> expressions = Arrays.asList(
                "sum(dsSingle.m1) group by time",
                "sum(dsSingle) group by time",
                "sum(dsSingle) group by dsSingle.time",
                "sum(dsSingle.m1) group by dsSingle.time",
                "sum(dsSingle) along geo",
                "sum(dsSingle) along dsSingle.geo",
                "sum(dsSingle.m1) along geo",
                "sum(dsSingle.m1) along dsSingle.geo"
        );
        for (String expression : expressions) {

            AggregationOperation result = visitor.visit(parse(expression));

            DataStructure dataStructure = result.getDataStructure();
            Map<String, Component.Role> roles = result.getDataStructure().getRoles();
            softly.assertThat(roles).contains(
                    entry("time", Component.Role.IDENTIFIER),
                    entry("m1", Component.Role.MEASURE)
            );

            softly.assertThat(result.getDataStructure().getTypes()).contains(
                    entry("time", String.class),
                    entry("m1", Long.class)
            );


            softly.assertThat(result.getData()).contains(
                    dataStructure.wrap(ImmutableMap.of("time", "2010", "m1", 20L+40L+60L)),
                    dataStructure.wrap(ImmutableMap.of("time", "2011", "m1", 11L+31L+51L)),
                    dataStructure.wrap(ImmutableMap.of("time", "2012", "m1", 72L+82L+92L))
            );

        }
    }

    @Test
    // TODO: Move this to its own test when aggregation functions are improved
    public void testSumSingleMeasureDataSet() {

        List<Component> groupBy = Lists.newArrayList(datasetSingleMeasure.getDataStructure().getOrDefault("time", null));
        List<Component> components = datasetSingleMeasure.getDataStructure().values().stream()
                .filter(Component::isMeasure)
                .collect(Collectors.toList());
        AggregationOperation sumOperation = AggregationVisitor.getAggregationOperation(
                datasetSingleMeasure,groupBy, components, new AggregationSumFunction());
        sumOperation.getData().forEach(System.out::println);

        DataStructure dataStructure = sumOperation.getDataStructure();

        assertThat(dataStructure.getRoles()).contains(
                entry("time", Component.Role.IDENTIFIER),
                entry("m1", Component.Role.MEASURE)
        );

        assertThat(dataStructure.getTypes()).contains(
                entry("time", String.class),
                entry("m1", Long.class)
        );


        assertThat(sumOperation.getData()).contains(
                dataStructure.wrap(ImmutableMap.of("time", "2010", "m1", 20L+40L+60L)),
                dataStructure.wrap(ImmutableMap.of("time", "2011", "m1", 11L+31L+51L)),
                dataStructure.wrap(ImmutableMap.of("time", "2012", "m1", 72L+82L+92L))
        );

    }


    @Test
    // TODO: Move this to its own test when aggregation functions are improved
    public void testSumMultiMeasureDataSetAll() {
        List<Component> groupBy = Lists.newArrayList(datasetMultiMeasure.getDataStructure().getOrDefault("time", null));
        List<Component> components = datasetMultiMeasure.getDataStructure().values().stream()
                .filter(Component::isMeasure)
                .collect(Collectors.toList());
        AggregationOperation sumOperation = AggregationVisitor.getAggregationOperation(
                datasetMultiMeasure, groupBy, components, new AggregationSumFunction());

        DataStructure dataStructure = sumOperation.getDataStructure();

        assertThat(dataStructure.getRoles()).contains(
                entry("time", Component.Role.IDENTIFIER),
                entry("m1", Component.Role.MEASURE),
                entry("m2", Component.Role.MEASURE)
        );

        assertThat(dataStructure.getTypes()).contains(
                entry("time", String.class),
                entry("m1", Long.class),
                entry("m2", Long.class)
        );

        assertThat(sumOperation.getData()).contains(
                dataStructure.wrap(ImmutableMap.of("time", "2010", "m1", 20L+40L+60L, "m2", 2L+4L+6L )),
                dataStructure.wrap(ImmutableMap.of("time", "2011", "m1", 11L+31L+51L, "m2", 1L+3L+5L)),
                dataStructure.wrap(ImmutableMap.of("time", "2012", "m1", 72L+82L+92L, "m2", 7L+8L+9L))
        );
    }

    @Test
    // TODO: Move this to its own test when aggregation functions are improved
    public void testSumMultiMeasureDataSet() {

        DataStructure dataStructure = datasetMultiMeasure.getDataStructure();
        Component m1 = dataStructure.getOrDefault("m1", null);
        List<Component> groupBy = Lists.newArrayList(dataStructure.getOrDefault("time", null));

        AggregationOperation sumOperation = AggregationVisitor.getAggregationOperation(
                datasetMultiMeasure, groupBy, Collections.singletonList(m1), new AggregationSumFunction());

        DataStructure resultingDataStructure = sumOperation.getDataStructure();

        assertThat(resultingDataStructure.getRoles()).contains(
                entry("time", Component.Role.IDENTIFIER),
                entry("m1", Component.Role.MEASURE)
        );

        assertThat(resultingDataStructure.getTypes()).contains(
                entry("time", String.class),
                entry("m1", Long.class)
        );

        assertThat(sumOperation.getData()).contains(
                resultingDataStructure.wrap(ImmutableMap.of("time", "2010", "m1", 20L+40L+60L)),
                resultingDataStructure.wrap(ImmutableMap.of("time", "2011", "m1", 11L+31L+51L)),
                resultingDataStructure.wrap(ImmutableMap.of("time", "2012", "m1", 72L+82L+92L))
        );
    }

    @Test
    // TODO: Move this to its own test when aggregation functions are improved
    public void testAvgMultiMeasureDataSet() {
        DataStructure dataStructure = datasetMultiMeasure.getDataStructure();
        Component m1 = dataStructure.getOrDefault("m1", null);
        List<Component> groupBy = Lists.newArrayList(dataStructure.getOrDefault("time", null));

        AggregationOperation avgOperation = AggregationVisitor.getAggregationOperation(
                datasetMultiMeasure, groupBy, Collections.singletonList(m1), new AggregationAvgFunction());

        DataStructure resultingDataStructure = avgOperation.getDataStructure();

        assertThat(resultingDataStructure.getRoles()).contains(
                entry("time", Component.Role.IDENTIFIER),
                entry("m1", Component.Role.MEASURE)
        );

        assertThat(resultingDataStructure.getTypes()).contains(
                entry("time", String.class),
                entry("m1", Long.class)
        );

        assertThat(avgOperation.getData()).contains(
                resultingDataStructure.wrap(ImmutableMap.of("time", "2010", "m1", (20L+40L+60L)/3F)),
                resultingDataStructure.wrap(ImmutableMap.of("time", "2011", "m1", (11L+31L+51L)/3F)),
                resultingDataStructure.wrap(ImmutableMap.of("time", "2012", "m1", (72L+82L+92L)/3F))
        );
    }

    @Test
    // TODO: Move this to its own test when aggregation functions are improved
    public void testAvgMultiMeasureDataSetWithNullValues() {
        DataStructure dataStructure = datasetMultiMeasureWithNullValues.getDataStructure();
        Component m1 = dataStructure.getOrDefault("m1", null);
        List<Component> groupBy = Lists.newArrayList(dataStructure.getOrDefault("time", null));

        AggregationOperation avgOperation = AggregationVisitor.getAggregationOperation(
                datasetMultiMeasureWithNullValues,groupBy, Collections.singletonList(m1), new AggregationAvgFunction());

        DataStructure resultingDataStructure = avgOperation.getDataStructure();

        assertThat(resultingDataStructure.getRoles()).contains(
                entry("time", Component.Role.IDENTIFIER),
                entry("m1", Component.Role.MEASURE)
        );

        assertThat(resultingDataStructure.getTypes()).contains(
                entry("time", String.class),
                entry("m1", Long.class)
        );

        assertThat(avgOperation.getData()).contains(
                resultingDataStructure.wrap(ImmutableMap.of("time", "2010", "m1", (20L+40L)/2F)),
                resultingDataStructure.wrap(ImmutableMap.of("time", "2011", "m1", (11L+31L+51L)/3F)),
                resultingDataStructure.wrap(ImmutableMap.of("time", "2012", "m1", (72L)/1F))
        );
    }

    @Test
    // TODO: Move this to its own test when aggregation functions are improved
    public void testAvgGroupedByMultipleIdentifiers() {
        DataStructure dataStructure = datasetSingleMeasure.getDataStructure();
        List<Component> groupBy = Lists.newArrayList(dataStructure.get("time"), dataStructure.get("geo"));
        System.out.println("Group By " + groupBy);
        List<Component> components = datasetSingleMeasure.getDataStructure().values().stream()
                .filter(Component::isMeasure)
                .collect(Collectors.toList());
        AggregationOperation avgOperation = AggregationVisitor.getAggregationOperation(
                datasetSingleMeasure, groupBy, components, new AggregationAvgFunction());
        avgOperation.getData().forEach(System.out::println);

        DataStructure resultingDataStructure = avgOperation.getDataStructure();

        assertThat(resultingDataStructure.getRoles()).contains(
                entry("time", Component.Role.IDENTIFIER),
                entry("m1", Component.Role.MEASURE)
        );

        assertThat(resultingDataStructure.getTypes()).contains(
                entry("time", String.class),
                entry("m1", Long.class)
        );

        assertThat(avgOperation.getData()).contains(
                resultingDataStructure.wrap(ImmutableMap.of("time", "2010", "geo", "DK", "m1", 60D)),
                resultingDataStructure.wrap(ImmutableMap.of("time", "2010", "geo", "NO", "m1", 20D)),
                resultingDataStructure.wrap(ImmutableMap.of("time", "2010", "geo", "SE", "m1", 40D)),
                resultingDataStructure.wrap(ImmutableMap.of("time", "2011", "geo", "DK", "m1", 51D)),
                resultingDataStructure.wrap(ImmutableMap.of("time", "2011", "geo", "NO", "m1", 11D)),
                resultingDataStructure.wrap(ImmutableMap.of("time", "2011", "geo", "SE", "m1", 31D)),
                resultingDataStructure.wrap(ImmutableMap.of("time", "2012", "geo", "DK", "m1", 92D)),
                resultingDataStructure.wrap(ImmutableMap.of("time", "2012", "geo", "NO", "m1", 72D)),
                resultingDataStructure.wrap(ImmutableMap.of("time", "2012", "geo", "SE", "m1", 41D))
        );
    }

    @Test
    // TODO: Move this to its own test when aggregation functions are improved
    public void testSumGroupedByMultipleIdentifiers() {
        DataStructure dataStructure = datasetSingleMeasure.getDataStructure();
        List<Component> groupBy = Lists.newArrayList(dataStructure.get("time"), dataStructure.get("geo"));
        System.out.println("Group By " + groupBy);
        List<Component> components = datasetSingleMeasure.getDataStructure().values().stream()
                .filter(Component::isMeasure)
                .collect(Collectors.toList());
        AggregationOperation sumOperation = AggregationVisitor.getAggregationOperation(
                datasetSingleMeasure, groupBy, components, new AggregationSumFunction());
        sumOperation.getData().forEach(System.out::println);

        DataStructure resultingDataStructure = sumOperation.getDataStructure();

        assertThat(resultingDataStructure.getRoles()).contains(
                entry("time", Component.Role.IDENTIFIER),
                entry("m1", Component.Role.MEASURE)
        );

        assertThat(resultingDataStructure.getTypes()).contains(
                entry("time", String.class),
                entry("m1", Long.class)
        );

        assertThat(sumOperation.getData()).contains(
                resultingDataStructure.wrap(ImmutableMap.of("time", "2010", "geo", "NO", "m1", 20L)),
                resultingDataStructure.wrap(ImmutableMap.of("time", "2010", "geo", "SE", "m1", 40L)),
                resultingDataStructure.wrap(ImmutableMap.of("time", "2010", "geo", "DK", "m1", 60L)),
                resultingDataStructure.wrap(ImmutableMap.of("time", "2011", "geo", "NO", "m1", 11L)),
                resultingDataStructure.wrap(ImmutableMap.of("time", "2011", "geo", "SE", "m1", 31L)),
                resultingDataStructure.wrap(ImmutableMap.of("time", "2011", "geo", "DK", "m1", 51L)),
                resultingDataStructure.wrap(ImmutableMap.of("time", "2012", "geo", "NO", "m1", 72L)),
                resultingDataStructure.wrap(ImmutableMap.of("time", "2012", "geo", "SE", "m1", 82L)),
                resultingDataStructure.wrap(ImmutableMap.of("time", "2012", "geo", "DK", "m1", 92L))
        );
    }

    @Test
    // TODO: Move this to its own test when aggregation functions are improved
    public void testAvgAlongMultipleIdentifiers() {
        Dataset datasetToBeAvged = createMultipleIdentifiersDataset();

        DataStructure dataStructure = datasetToBeAvged.getDataStructure();

        List<Component> alongComponents = Lists.newArrayList(dataStructure.get("enhet"), dataStructure.get("region"));
        List<Component> groupBy = dataStructure.values()
                .stream()
                .filter(Component::isIdentifier)
                .filter(component -> !alongComponents.contains(component))
                .collect(Collectors.toList());
        System.out.println(groupBy);
        List<Component> components = datasetToBeAvged.getDataStructure().values().stream()
                .filter(Component::isMeasure)
                .collect(Collectors.toList());
        AggregationOperation avgOperation = AggregationVisitor.getAggregationOperation(
                datasetToBeAvged,
                groupBy,
                components,
                new AggregationAvgFunction());

        assertThat(avgOperation.getData()).containsOnly(dataPoint("EIER", "F130KFEIER", "130", "2015", "SBDR", (8418D+1092D+5367D+4370D+318D+610D+3888D+24D)/10));
    }

    @Test
    // TODO: Move this to its own test when aggregation functions are improved
    public void testSumAlongMultipleIdentifiers() {
        Dataset datasetToBeSummed = createMultipleIdentifiersDataset();

        DataStructure dataStructure = datasetToBeSummed.getDataStructure();

        List<Component> alongComponents = Lists.newArrayList(dataStructure.get("enhet"), dataStructure.get("region"));
        List<Component> groupBy = dataStructure.values()
                .stream()
                .filter(Component::isIdentifier)
                .filter(component -> !alongComponents.contains(component))
                .collect(Collectors.toList());
        System.out.println(groupBy);
        List<Component> components = datasetToBeSummed.getDataStructure().values().stream()
                .filter(Component::isMeasure)
                .collect(Collectors.toList());
        AggregationOperation sumOperation = AggregationVisitor.getAggregationOperation(
                datasetToBeSummed,
                groupBy,
                components,
                new AggregationSumFunction());

        assertThat(sumOperation.getData()).containsOnly(dataPoint("EIER", "F130KFEIER", "130", "2015", "SBDR", 8418L+1092L+5367L+4370L+318L+610L+3888L+24L));
    }

    private Dataset createMultipleIdentifiersDataset() {
        return StaticDataset.create()
                .addComponent("eieform", Component.Role.IDENTIFIER, String.class)
                .addComponent("enhet", Component.Role.IDENTIFIER, String.class)
                .addComponent("feltnavn", Component.Role.IDENTIFIER, String.class)
                .addComponent("funksjon", Component.Role.IDENTIFIER, String.class)
                .addComponent("periode", Component.Role.IDENTIFIER, String.class)
                .addComponent("region", Component.Role.IDENTIFIER, String.class)
                .addComponent("regnskapsomfang", Component.Role.IDENTIFIER, String.class)
                .addComponent("areal", Component.Role.MEASURE, Long.class)

                .addPoints("EIER", "981548183", "F130KFEIER", "130", "2015", "0104", "SBDR", 8418L)
                .addPoints("EIER", "988935816", "F130KFEIER", "130", "2015", "0105", "SBDR", 0L)
                .addPoints("EIER", "979952171", "F130KFEIER", "130", "2015", "0106", "SBDR", 1092L)
                .addPoints("EIER", "986386947", "F130KFEIER", "130", "2015", "0226", "SBDR", 5367L)
                .addPoints("EIER", "994860216", "F130KFEIER", "130", "2015", "0233", "SBDR", 4370L)
                .addPoints("EIER", "997756215", "F130KFEIER", "130", "2015", "0236", "SBDR", 318L)
                .addPoints("EIER", "984070659", "F130KFEIER", "130", "2015", "0301", "SBDR", 610L)
                .addPoints("EIER", "985987246", "F130KFEIER", "130", "2015", "0301", "SBDR", 3888L)
                .addPoints("EIER", "987592567", "F130KFEIER", "130", "2015", "0301", "SBDR", 24L)
                .addPoints("EIER", "983529968", "F130KFEIER", "130", "2015", "0417", "SBDR", 0L)
                .build();
    }

    @Test
    // TODO: Move this to its own test when aggregation functions are improved
    public void testAvgWithNullValues() {
        Dataset dataset = createDatasetWithNullValues();

        List<Component> components = dataStructureSingleMeasure.values().stream()
                .filter(Component::isMeasure)
                .collect(Collectors.toList());

        AggregationOperation avgOperation = AggregationVisitor.getAggregationOperation(
                dataset,
                Collections.singletonList(dataStructureSingleMeasure.get("time")),
                components,
                new AggregationAvgFunction());
        assertThat(avgOperation.getData()).contains(dataPoint("2012", (41D + 92D)/2));
    }

    @Test
    // TODO: Move this to its own test when aggregation functions are improved
    public void testSumWithNullValues() {
        Dataset dataset = createDatasetWithNullValues();

        List<Component> components = dataStructureSingleMeasure.values().stream()
                .filter(Component::isMeasure)
                .collect(Collectors.toList());

        AggregationOperation sumOperation = AggregationVisitor.getAggregationOperation(
                dataset,
                Collections.singletonList(dataStructureSingleMeasure.get("time")),
                components,
                new AggregationSumFunction());
        assertThat(sumOperation.getData()).contains(dataPoint("2012", 41L + 92L));
    }

    private Dataset createDatasetWithNullValues() {
        return StaticDataset.create(dataStructureSingleMeasure)
                .addPoints("2010", "NO", 20L)
                .addPoints("2011", "SE", 31L)
                .addPoints("2012", "SE", null)
                .addPoints("2012", "NO", null)
                .addPoints("2010", "SE", 40L)
                .addPoints("2011", "NO", 11L)
                .addPoints("2012", "SE", 41L)
                .addPoints("2010", "DK", null)
                .addPoints("2011", "DK", 51L)
                .addPoints("2012", "DK", 92L)
                .build();
    }

    @Test(expected = ParseCancellationException.class)
    // TODO: Move this to its own test when aggregation functions are improved
    // TODO: Use assertThrownBy with ContextualRuntimeException.
    public void testAggregationAvgWithoutNumber() {
        Dataset dataset = StaticDataset.create()
                .addComponent("id1", Component.Role.IDENTIFIER, String.class)
                .addComponent("m1", Component.Role.MEASURE, String.class)
                .addPoints("1", "notANumeric")
                .addPoints("2", "shouldFail")
                .build();

        List<Component> components = dataset.getDataStructure().values().stream()
                .filter(Component::isMeasure)
                .collect(Collectors.toList());

        AggregationOperation avgOperation = AggregationVisitor.getAggregationOperation(
                dataset,
                Collections.singletonList(dataStructureSingleMeasure.get("time")),
                components,
                new AggregationAvgFunction());
        avgOperation.getDataStructure();
        fail("Expected an exception but none was thrown");
    }

    @Test(expected = ParseCancellationException.class)
    // TODO: Move this to its own test when aggregation functions are improved
    // TODO: Use assertThrownBy with ContextualRuntimeException.
    public void testAggregationSumWithoutNumber() {
        Dataset dataset = StaticDataset.create()
                .addComponent("id1", Component.Role.IDENTIFIER, String.class)
                .addComponent("m1", Component.Role.MEASURE, String.class)
                .addPoints("1", "notANumeric")
                .addPoints("2", "shouldFail")
                .build();

        List<Component> components = dataset.getDataStructure().values().stream()
                .filter(Component::isMeasure)
                .collect(Collectors.toList());

        AggregationOperation sumOperation = AggregationVisitor.getAggregationOperation(
                dataset,
                Collections.singletonList(dataStructureSingleMeasure.get("time")),
                components,
                new AggregationSumFunction());
        sumOperation.getDataStructure();
        fail("Expected an exception but none was thrown");
    }

    @Test
    // TODO: Move this to its own test when aggregation functions are improved
    public void testAvgWithEmptyAggregationGroup() {
        //dataset with several null values. In fact ALL 2010 values are null
        Dataset dataset = createDatasetWithSeveralNullValues();

        List<Component> components = dataset.getDataStructure().values().stream()
                .filter(Component::isMeasure)
                .collect(Collectors.toList());

        AggregationOperation avgOperation = AggregationVisitor.getAggregationOperation(
                dataset,
                Collections.singletonList(dataStructureSingleMeasure.get("time")),
                components,
                new AggregationAvgFunction());
        assertThat(avgOperation.getData()).contains(dataPoint("2012", (41D + 92D)/2));
        assertThat(avgOperation.getData()).contains(dataPoint("2010", null));
    }

    @Test
    // TODO: Move this to its own test when aggregation functions are improved
    public void testSumWithEmptyAggregationGroup() {
        //dataset with several null values. In fact ALL 2010 values are null
        Dataset dataset = createDatasetWithSeveralNullValues();

        List<Component> components = dataset.getDataStructure().values().stream()
                .filter(Component::isMeasure)
                .collect(Collectors.toList());

        AggregationOperation sumOperation = AggregationVisitor.getAggregationOperation(
                dataset,
                Collections.singletonList(dataStructureSingleMeasure.get("time")),
                components,
                new AggregationSumFunction());
        assertThat(sumOperation.getData()).contains(dataPoint("2012", 41L + 92L));
        assertThat(sumOperation.getData()).contains(dataPoint("2010", null));
    }

    private Dataset createDatasetWithSeveralNullValues() {
        return StaticDataset.create(dataStructureSingleMeasure)
                .addPoints("2010", "NO", null)
                .addPoints("2011", "SE", 31L)
                .addPoints("2012", "SE", null)
                .addPoints("2012", "NO", null)
                .addPoints("2010", "SE", null)
                .addPoints("2011", "NO", 11L)
                .addPoints("2012", "SE", 41L)
                .addPoints("2010", "DK", null)
                .addPoints("2011", "DK", 51L)
                .addPoints("2012", "DK", 92L)
                .build();
    }

    private DataPoint dataPoint(Object... objects) {
        List<VTLObject> vtlObjects = Stream.of(objects).map(VTLObject::of).collect(Collectors.toList());
        return DataPoint.create(vtlObjects);
    }
}
