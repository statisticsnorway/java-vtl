package no.ssb.vtl.script.visitors.join;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import no.ssb.vtl.model.Component;
import no.ssb.vtl.model.DataStructure;
import no.ssb.vtl.model.Dataset;
import org.assertj.core.api.SoftAssertions;
import org.junit.Test;

import java.util.Collections;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AbstractJoinOperationTest {

    ObjectMapper mapper = new ObjectMapper();

    /**
     * The specification states:
     * "A Dataset ds should appear only once in the list of Datasets"
     * <p>
     * This is preventing self joins. Which are very useful. This
     * test is therefore not asserting the specified behaviour.
     */
    @Test
    public void testSameDatasetShouldFail() throws Exception {
        SoftAssertions softly = new SoftAssertions();
        try {
            Dataset ds1 = mock(Dataset.class);
            Dataset ds2 = mock(Dataset.class);

            DataStructure structure = DataStructure.of(
                    mapper::convertValue,
                    "m",
                    Component.Role.IDENTIFIER,
                    Integer.class
            );

            when(ds1.getDataStructure()).thenReturn(structure);
            when(ds2.getDataStructure()).thenReturn(structure);

            Throwable ex;

            ex = null;
            try {

                new AbstractJoinOperation(
                        ImmutableMap.of("ds1", ds1, "ds2", ds2)
                ) {

                    @Override
                    WorkingDataset workDataset() {
                        return new WorkingDataset() {
                            @Override
                            public DataStructure getDataStructure() {
                                return ds1.getDataStructure();
                            }

                            @Override
                            public Stream<Tuple> get() {
                                return ds1.get();
                            }
                        };
                    }
                };
            } catch (Throwable t) {
                ex = t;
            }
            softly.assertThat(ex)
                    .as("exception thrown on control")
                    .isNull();


            ex = null;
            try {
                new AbstractJoinOperation(
                        ImmutableMap.of("ds1", ds1, "ds12", ds1)
                ) {

                    @Override
                    WorkingDataset workDataset() {
                        return null;
                    }
                };
            } catch (Throwable t) {
                ex = t;
            }
            softly.assertThat(ex)
                    .as("exception thrown when using same dataset" +
                            " more than once in a join operation")
                    // TODO: Reevaluate after the 1.1 release of the spec.
                    //.isNotNull();
                    .isNull();

        } finally {
            softly.assertAll();
        }
    }

    @Test
    public void testEmptyFails() throws Exception {
        Throwable ex = null;
        try {
            new AbstractJoinOperation(Collections.emptyMap()) {
                @Override
                WorkingDataset workDataset() {
                    return null;
                }
            };
        } catch (Throwable t) {
            ex = t;
        }
        assertThat(ex)
                .hasMessageContaining("join")
                .hasMessageContaining("empty")
                .hasMessageContaining("dataset");
    }

    @Test
    public void testSimple() throws Exception {

        Dataset ds1 = mock(Dataset.class);

        DataStructure ds1Struct = DataStructure.of(
                mapper::convertValue,
                "m",
                Component.Role.IDENTIFIER,
                Integer.class
        );

        given(ds1.getDataStructure()).willReturn(ds1Struct);
        given(ds1.get()).will(invocation -> {
            return IntStream.rangeClosed(0, 10).boxed().map(
                    integer -> ds1Struct.wrap(
                            ImmutableMap.of(
                                    "m", integer
                            )
                    )
            );
        });
        AbstractJoinOperation result = new AbstractJoinOperation(ImmutableMap.of("ds1", ds1)) {

            @Override
            WorkingDataset workDataset() {
                return new WorkingDataset() {
                    @Override
                    public DataStructure getDataStructure() {
                        return ds1.getDataStructure();
                    }

                    @Override
                    public Stream<Tuple> get() {
                        return ds1.get();
                    }
                };
            }
        };

        result.getClauses().add(new JoinClause() {

            @Override
            public WorkingDataset apply(WorkingDataset workingDataset) {
                return new WorkingDataset() {
                    @Override
                    public DataStructure getDataStructure() {
                        return workingDataset.getDataStructure();
                    }

                    @Override
                    public Stream<Tuple> get() {
                        return workingDataset.get();
                    }
                };
            }

        });

        assertThat(result.get())
                .containsAll(ds1.get().collect(Collectors.toList()));

    }
}
