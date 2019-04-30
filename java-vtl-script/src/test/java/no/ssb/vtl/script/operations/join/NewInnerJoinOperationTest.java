package no.ssb.vtl.script.operations.join;

/*-
 * ========================LICENSE_START=================================
 * Java VTL
 * %%
 * Copyright (C) 2016 - 2018 Hadrien Kohl
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
import com.carrotsearch.randomizedtesting.annotations.Repeat;
import com.carrotsearch.randomizedtesting.annotations.Seed;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import no.ssb.vtl.model.Component;
import no.ssb.vtl.model.DataPoint;
import no.ssb.vtl.model.DataStructure;
import no.ssb.vtl.model.Dataset;
import no.ssb.vtl.model.StaticDataset;
import no.ssb.vtl.script.operations.drop.KeepOperation;
import no.ssb.vtl.test.RandomizedDataset;
import org.assertj.core.util.Lists;
import org.junit.Before;
import org.junit.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static no.ssb.vtl.model.Component.Role.ATTRIBUTE;
import static no.ssb.vtl.model.Component.Role.IDENTIFIER;
import static no.ssb.vtl.model.Component.Role.MEASURE;

public class NewInnerJoinOperationTest extends RandomizedTest {

    private DataStructure structureTemplate;

    @Before
    public void setUp() {
        structureTemplate = DataStructure.builder()

                .put("stringId", IDENTIFIER, String.class)
                .put("longId", IDENTIFIER, Long.class)
                .put("doubleId", IDENTIFIER, Double.class)
                .put("booleanId", IDENTIFIER, Boolean.class)
                .put("instantId", IDENTIFIER, Instant.class)

                .put("stringMeasure", MEASURE, String.class)
                .put("longMeasure", MEASURE, Long.class)
                .put("doubleMeasure", MEASURE, Double.class)
                .put("booleanMeasure", MEASURE, Boolean.class)
                .put("instantMeasure", MEASURE, Instant.class)

                .put("stringAttribute", ATTRIBUTE, String.class)
                .put("longAttribute", ATTRIBUTE, Long.class)
                .put("doubleAttribute", ATTRIBUTE, Double.class)
                .put("booleanAttribute", ATTRIBUTE, Boolean.class)
                .put("instantAttribute", ATTRIBUTE, Instant.class)

                .build();

    }

    private Object randomObjectOrNullOfType(Class clazz) {
        if (rarely())
            return null;
        return randomObjectOfType(clazz);
    }

    private Object randomObjectOfType(Class clazz) {
        if (Boolean.class.equals(clazz))
            return randomBoolean();
        if (String.class.equals(clazz))
            return randomAsciiOfLengthBetween(10, 20);
        if (Long.class.equals(clazz))
            return randomLong();
        if (Double.class.equals(clazz))
            return randomDouble();
        if (Instant.class.equals(clazz))
            return Instant.ofEpochMilli(randomLong());
        throw new IllegalArgumentException(
                "Could not generate random value of type " + clazz.getSimpleName()
        );
    }

    private List<DataPoint> randomData(DataStructure structure, int numRow) {
        ArrayList<DataPoint> data = Lists.newArrayList();
        ArrayList<Object> values = Lists.newArrayList();
        for (int i = 0; i < numRow; i++) {
            for (Component component : structure.values()) {
                values.add(randomObjectOrNullOfType(component.getType()));
            }
            data.add(DataPoint.create(values.toArray()));
            values.clear();
        }
        return data;
    }

    StaticDataset randomDatasetFromValues(DataStructure structure, List<DataPoint> data) {
        StaticDataset.ValueBuilder builder = StaticDataset.create(structure);
        ArrayList<DataPoint> copy = Lists.newArrayList(data);
        Collections.shuffle(copy, getRandom());
        int max = randomInt(data.size());
        for (int i = 0; i < max; i++) {
            builder.addPoints(data.get(i));
        }
        return builder.build();
    }

    List<Component> randomComponents(DataStructure structure, Predicate<Component> predicate, int min) {
        List<Component> collect = structure.values().stream()
                .filter(predicate)
                .collect(ImmutableList.toImmutableList());
        int max = randomIntBetween(min, collect.size());
        ImmutableList.Builder<Component> components = ImmutableList.builder();
        for (int i = 0; i < max; i++) {
            Component component = randomFrom(collect);
            components.add(component);
        }
        return components.build();
    }

    List<String> randomComponentsName(DataStructure structure, Predicate<Component> predicate, int min) {
        return randomComponents(structure, predicate, min).stream()
                .map(structure::getName)
                .collect(ImmutableList.toImmutableList());
    }

    List<String> randomIdentifiersName(DataStructure structure, int min) {
        return randomComponentsName(structure, Component::isIdentifier, min);
    }

    List<Component> randomIdentifiers(DataStructure structure, int min) {
        return randomComponents(structure, Component::isIdentifier, min);
    }

    List<String> randomMeasuresName(DataStructure structure, int min) {
        return randomComponentsName(structure, Component::isMeasure, min);
    }

    List<Component> randomMeasures(DataStructure structure, int min) {
        return randomComponents(structure, Component::isMeasure, min);
    }

    List<String> randomAttributesName(DataStructure structure, int min) {
        return randomComponentsName(structure, Component::isAttribute, min);
    }

    List<Component> randomAttributes(DataStructure structure, int min) {
        return randomComponents(structure, Component::isAttribute, min);
    }

    @Test
    @Repeat(iterations = 1, useConstantSeed = true)
    @Seed("D1F262C1DA20FE15")
    public void regressionTest() {
        testInnerJoin();
    }

    @Test
    @Repeat(iterations = 10)
    public void testInnerJoin() {
        // A couple of important things to test:
        // * variable amount of datasets
        // * variable amount of null value in identifiers
        // * random structure order
        // * random data order

        List<DataPoint> dataPoints = randomData(structureTemplate, scaledRandomIntBetween(10, 100));

        // Keep a list of identifiers that will be common.
        List<String> commonIdentifiers = randomIdentifiersName(structureTemplate, 1);
        System.out.println("Common identifier" + commonIdentifiers);
        Map<String, Dataset> datasetMap = IntStream.range(1, randomIntBetween(2, 3))
                .mapToObj(value -> "ds" + Integer.toString(value))
                .collect(ImmutableMap.toImmutableMap(
                        o -> o,
                        o -> {

                            // Rarely return empty dataset.
                            StaticDataset staticDataset = randomDatasetFromValues(
                                    structureTemplate,
                                    rarely() ? Collections.emptyList() : dataPoints
                            );
                            Dataset randomizedDataset = RandomizedDataset.create(getRandom(), staticDataset).shuffleStructure();


                            Set<Component> components = Stream.of(
                                    commonIdentifiers.stream(),
                                    randomMeasuresName(randomizedDataset.getDataStructure(), 1).stream(),
                                    randomAttributesName(randomizedDataset.getDataStructure(), 1).stream()
                            ).flatMap(s -> s)
                                    .map(randomizedDataset.getDataStructure()::get)
                                    .collect(ImmutableSet.toImmutableSet());

                            return new KeepOperation(randomizedDataset, components);
                        }
                ));

        InnerJoinOperation innerJoinOperation = new InnerJoinOperation(datasetMap);

        // Uncomment to debug.
        // VTLPrintStream printStream = new VTLPrintStream(System.out);
        // datasetMap.forEach((name, dataset) -> {
        //     printStream.println("Dataset " + name);
        //     printStream.println(dataset);
        // });
        // printStream.println(innerJoinOperation);



    }
}
