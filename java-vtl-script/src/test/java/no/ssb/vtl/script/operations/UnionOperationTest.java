package kohl.hadrien.vtl.script.operations;

import com.fasterxml.jackson.databind.ObjectMapper;
import no.ssb.vtl.model.DataPoint;
import no.ssb.vtl.model.DataStructure;
import no.ssb.vtl.model.Dataset;
import org.assertj.core.api.SoftAssertions;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static kohl.hadrien.vtl.model.Component.Role;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class UnionOperationTest {

    ObjectMapper mapper = new ObjectMapper();

    @Test
    public void testOneDatasetReturnedUnchanged() throws Exception {

        Dataset dataset = mock(Dataset.class);
        when(dataset.get()).thenReturn(Stream.empty());

        UnionOperation operator = new UnionOperation(dataset);

        assertThat(operator.get().get())
                .as("Check that result of union operation", null)
                .isSameAs(dataset.get());

    }


    @Test
    public void testSameIdentifierAndMeasures() throws Exception {

        SoftAssertions softly = new SoftAssertions();
        try {

            DataStructure dataStructure = DataStructure.of(mapper::convertValue,
                    "TIME", Role.IDENTIFIER, String.class,
                    "GEO", Role.IDENTIFIER, String.class,
                    "POP", Role.MEASURE, Integer.class
            );

            Dataset dataset1 = mock(Dataset.class);
            Dataset dataset2 = mock(Dataset.class);
            Dataset dataset3 = mock(Dataset.class);

            when(dataset1.getDataStructure()).thenReturn(dataStructure);
            when(dataset2.getDataStructure()).thenReturn(dataStructure);
            when(dataset3.getDataStructure()).thenReturn(dataStructure);

            UnionOperation unionOperation = new UnionOperation(dataset1, dataset2, dataset3);
            softly.assertThat(unionOperation).isNotNull();

            DataStructure wrongStructure = DataStructure.of(mapper::convertValue,
                    "TIME2", Role.IDENTIFIER, String.class,
                    "GEO2", Role.IDENTIFIER, String.class,
                    "POP2", Role.MEASURE, Integer.class
            );

            Dataset wrongDataset = mock(Dataset.class);
            when(wrongDataset.getDataStructure()).thenReturn(wrongStructure);
            Throwable expextedEx = null;
            try {
                new UnionOperation(dataset1, wrongDataset, dataset2, dataset3);
            } catch (Throwable t) {
                expextedEx = t;
            }
            softly.assertThat(expextedEx).isNotNull().hasMessageContaining("incompatible");

        } finally {
            softly.assertAll();
        }
    }

    @Test
    public void testUnion() throws Exception {

        // Example 1 of the operator specification

        DataStructure dataStructure = DataStructure.of(mapper::convertValue,
                "TIME", Role.IDENTIFIER, String.class,
                "GEO", Role.IDENTIFIER, String.class,
                "POP", Role.MEASURE, Integer.class
        );

        Dataset totalPopulation1 = mock(Dataset.class);
        Dataset totalPopulation2 = mock(Dataset.class);
        when(totalPopulation1.getDataStructure()).thenReturn(dataStructure);
        when(totalPopulation2.getDataStructure()).thenReturn(dataStructure);

        when(totalPopulation1.get()).thenReturn(Stream.of(
                tuple(
                        dataStructure.wrap("TIME", "2012"),
                        dataStructure.wrap("GEO", "Belgium"),
                        dataStructure.wrap("POP", 5)
                ), tuple(
                        dataStructure.wrap("TIME", "2012"),
                        dataStructure.wrap("GEO", "Greece"),
                        dataStructure.wrap("POP", 2)
                ), tuple(
                        dataStructure.wrap("TIME", "2012"),
                        dataStructure.wrap("GEO", "France"),
                        dataStructure.wrap("POP", 3)
                ), tuple(
                        dataStructure.wrap("TIME", "2012"),
                        dataStructure.wrap("GEO", "Malta"),
                        dataStructure.wrap("POP", 7)
                ), tuple(
                        dataStructure.wrap("TIME", "2012"),
                        dataStructure.wrap("GEO", "Finland"),
                        dataStructure.wrap("POP", 9)
                ), tuple(
                        dataStructure.wrap("TIME", "2012"),
                        dataStructure.wrap("GEO", "Switzerland"),
                        dataStructure.wrap("POP", 12)
                )
        ));

        when(totalPopulation2.get()).thenReturn(Stream.of(
                tuple(
                        dataStructure.wrap("TIME", "2012"),
                        dataStructure.wrap("GEO", "Netherlands"),
                        dataStructure.wrap("POP", 23)
                ), tuple(
                        dataStructure.wrap("TIME", "2012"),
                        dataStructure.wrap("GEO", "Greece"),
                        dataStructure.wrap("POP", 2)
                ), tuple(
                        dataStructure.wrap("TIME", "2012"),
                        dataStructure.wrap("GEO", "Spain"),
                        dataStructure.wrap("POP", 5)
                ), tuple(
                        dataStructure.wrap("TIME", "2012"),
                        dataStructure.wrap("GEO", "Iceland"),
                        dataStructure.wrap("POP", 1)
                )
        ));

        UnionOperation unionOperation = new UnionOperation(totalPopulation1, totalPopulation2);

        Dataset resultDataset = unionOperation.get();
        assertThat(resultDataset).isNotNull();

        Stream<Dataset.Tuple> stream = resultDataset.stream();
        assertThat(stream).isNotNull();

        assertThat(stream)
                .contains(
                        tuple(
                                dataStructure.wrap("TIME", "2012"),
                                dataStructure.wrap("GEO", "Belgium"),
                                dataStructure.wrap("POP", 5)
                        ), tuple(
                                dataStructure.wrap("TIME", "2012"),
                                dataStructure.wrap("GEO", "Greece"),
                                dataStructure.wrap("POP", 2)
                        ), tuple(
                                dataStructure.wrap("TIME", "2012"),
                                dataStructure.wrap("GEO", "France"),
                                dataStructure.wrap("POP", 3)
                        ), tuple(
                                dataStructure.wrap("TIME", "2012"),
                                dataStructure.wrap("GEO", "Malta"),
                                dataStructure.wrap("POP", 7)
                        ), tuple(
                                dataStructure.wrap("TIME", "2012"),
                                dataStructure.wrap("GEO", "Finland"),
                                dataStructure.wrap("POP", 9)
                        ), tuple(
                                dataStructure.wrap("TIME", "2012"),
                                dataStructure.wrap("GEO", "Switzerland"),
                                dataStructure.wrap("POP", 12)
                        ), tuple(
                                dataStructure.wrap("TIME", "2012"),
                                dataStructure.wrap("GEO", "Netherlands"),
                                dataStructure.wrap("POP", 23)
                        ), tuple(
                                dataStructure.wrap("TIME", "2012"),
                                dataStructure.wrap("GEO", "Spain"),
                                dataStructure.wrap("POP", 5)
                        ), tuple(
                                dataStructure.wrap("TIME", "2012"),
                                dataStructure.wrap("GEO", "Iceland"),
                                dataStructure.wrap("POP", 1)
                        )
                );
    }

    @Test
    public void testUnion2() throws Exception {

        // Example 2 of the operator specification.

        DataStructure dataStructure = DataStructure.of(mapper::convertValue,
                "TIME", Role.IDENTIFIER, String.class,
                "GEO", Role.IDENTIFIER, String.class,
                "POP", Role.MEASURE, Integer.class
        );

        Dataset totalPopulation1 = mock(Dataset.class);
        Dataset totalPopulation2 = mock(Dataset.class);
        when(totalPopulation1.getDataStructure()).thenReturn(dataStructure);
        when(totalPopulation2.getDataStructure()).thenReturn(dataStructure);

        when(totalPopulation1.get()).thenReturn(Stream.of(
                tuple(
                        dataStructure.wrap("TIME", "2012"),
                        dataStructure.wrap("GEO", "Belgium"),
                        dataStructure.wrap("POP", 1)
                ), tuple(
                        dataStructure.wrap("TIME", "2012"),
                        dataStructure.wrap("GEO", "Greece"),
                        dataStructure.wrap("POP", 2)
                ), tuple(
                        dataStructure.wrap("TIME", "2012"),
                        dataStructure.wrap("GEO", "France"),
                        dataStructure.wrap("POP", 3)
                ), tuple(
                        dataStructure.wrap("TIME", "2012"),
                        dataStructure.wrap("GEO", "Malta"),
                        dataStructure.wrap("POP", 4)
                ), tuple(
                        dataStructure.wrap("TIME", "2012"),
                        dataStructure.wrap("GEO", "Finland"),
                        dataStructure.wrap("POP", 5)
                ), tuple(
                        dataStructure.wrap("TIME", "2012"),
                        dataStructure.wrap("GEO", "Switzerland"),
                        dataStructure.wrap("POP", 6)
                )
        ));

        when(totalPopulation2.get()).thenReturn(Stream.of(
                tuple(
                        dataStructure.wrap("TIME", "2011"),
                        dataStructure.wrap("GEO", "Belgium"),
                        dataStructure.wrap("POP", 10)
                ), tuple(
                        dataStructure.wrap("TIME", "2011"),
                        dataStructure.wrap("GEO", "Greece"),
                        dataStructure.wrap("POP", 20)
                ), tuple(
                        dataStructure.wrap("TIME", "2011"),
                        dataStructure.wrap("GEO", "France"),
                        dataStructure.wrap("POP", 30)
                ), tuple(
                        dataStructure.wrap("TIME", "2011"),
                        dataStructure.wrap("GEO", "Malta"),
                        dataStructure.wrap("POP", 40)
                ), tuple(
                        dataStructure.wrap("TIME", "2011"),
                        dataStructure.wrap("GEO", "Finland"),
                        dataStructure.wrap("POP", 50)
                ), tuple(
                        dataStructure.wrap("TIME", "2011"),
                        dataStructure.wrap("GEO", "Switzerland"),
                        dataStructure.wrap("POP", 60)
                )
        ));

        UnionOperation unionOperation = new UnionOperation(totalPopulation1, totalPopulation2);

        Dataset resultDataset = unionOperation.get();
        assertThat(resultDataset).isNotNull();

        Stream<Dataset.Tuple> stream = resultDataset.stream();
        assertThat(stream).isNotNull();

        assertThat(stream)
                .contains(
                        tuple(
                                dataStructure.wrap("TIME", "2012"),
                                dataStructure.wrap("GEO", "Belgium"),
                                dataStructure.wrap("POP", 1)
                        ), tuple(
                                dataStructure.wrap("TIME", "2012"),
                                dataStructure.wrap("GEO", "Greece"),
                                dataStructure.wrap("POP", 2)
                        ), tuple(
                                dataStructure.wrap("TIME", "2012"),
                                dataStructure.wrap("GEO", "France"),
                                dataStructure.wrap("POP", 3)
                        ), tuple(
                                dataStructure.wrap("TIME", "2012"),
                                dataStructure.wrap("GEO", "Malta"),
                                dataStructure.wrap("POP", 4)
                        ), tuple(
                                dataStructure.wrap("TIME", "2012"),
                                dataStructure.wrap("GEO", "Finland"),
                                dataStructure.wrap("POP", 5)
                        ), tuple(
                                dataStructure.wrap("TIME", "2012"),
                                dataStructure.wrap("GEO", "Switzerland"),
                                dataStructure.wrap("POP", 6)
                        ),
                        tuple(
                                dataStructure.wrap("TIME", "2011"),
                                dataStructure.wrap("GEO", "Belgium"),
                                dataStructure.wrap("POP", 10)
                        ), tuple(
                                dataStructure.wrap("TIME", "2011"),
                                dataStructure.wrap("GEO", "Greece"),
                                dataStructure.wrap("POP", 20)
                        ), tuple(
                                dataStructure.wrap("TIME", "2011"),
                                dataStructure.wrap("GEO", "France"),
                                dataStructure.wrap("POP", 30)
                        ), tuple(
                                dataStructure.wrap("TIME", "2011"),
                                dataStructure.wrap("GEO", "Malta"),
                                dataStructure.wrap("POP", 40)
                        ), tuple(
                                dataStructure.wrap("TIME", "2011"),
                                dataStructure.wrap("GEO", "Finland"),
                                dataStructure.wrap("POP", 50)
                        ), tuple(
                                dataStructure.wrap("TIME", "2011"),
                                dataStructure.wrap("GEO", "Switzerland"),
                                dataStructure.wrap("POP", 60)
                        )
                );

    }

    private Dataset.Tuple tuple(DataPoint... components) {
        return new Dataset.AbstractTuple() {
            @Override
            protected List<DataPoint> delegate() {
                return Arrays.asList(components);
            }
        };
    }
}
