package kohl.hadrien.vtl.script.operations;

import com.codepoetics.protonpack.StreamUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import no.ssb.vtl.model.DataPoint;
import no.ssb.vtl.model.DataStructure;
import no.ssb.vtl.model.Dataset;
import kohl.hadrien.vtl.script.operations.join.InnerJoinOperation;
import kohl.hadrien.vtl.script.operations.join.JoinClause;
import kohl.hadrien.vtl.script.operations.join.WorkingDataset;
import org.assertj.core.api.SoftAssertions;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static kohl.hadrien.vtl.model.Component.Role;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by hadrien on 21/11/2016.
 */
public class SumOperationTest {

    ObjectMapper mapper = new ObjectMapper();

    /**
     * Both Datasets must have at least one Identifier Component
     * in common (with the same name and data type).
     * <p>
     * VTL 1.1 line 2499.
     */
    //@Test
    public void testIdenfitierNotASubset() throws Exception {

        Dataset left = mock(Dataset.class);
        Dataset right = mock(Dataset.class);

        SoftAssertions softly = new SoftAssertions();
        try {
            when(left.getDataStructure()).thenReturn(DataStructure.of(mapper::convertValue,
                    "ID1", Role.IDENTIFIER, String.class,
                    "ID2", Role.IDENTIFIER, String.class,
                    "ME1", Role.MEASURE, Integer.class
            ));
            Throwable expectedThrowable = null;

            // Different name
            when(right.getDataStructure()).thenReturn(DataStructure.of(mapper::convertValue,
                    "ID1DIFFERENTNAME", Role.IDENTIFIER, String.class,
                    "ID2", Role.IDENTIFIER, String.class,
                    "ME1", Role.MEASURE, Integer.class
            ));
            expectedThrowable = null;
            try {
                new SumOperation(tuple -> null, left.getDataStructure(), tuple -> null, right.getDataStructure());
            } catch (Throwable t) {
                expectedThrowable = t;
            }
            softly.assertThat(expectedThrowable)
                    .as("SumOperation exception when no common id name")
                    .isNotNull()
                    .hasMessageContaining("ID1DIFFERENTNAME");

            // any order
            expectedThrowable = null;
            try {
                new SumOperation(tuple -> null, right.getDataStructure(), tuple -> null, left.getDataStructure());
            } catch (Throwable t) {
                expectedThrowable = t;
            }
            softly.assertThat(expectedThrowable)
                    .as("SumOperation exception when no common id name (reversed)")
                    .isNotNull()
                    .hasMessageContaining("ID1DIFFERENTNAME");

            // Different type
            when(left.getDataStructure()).thenReturn(DataStructure.of(mapper::convertValue,
                    "ID1", Role.IDENTIFIER, String.class,
                    "ID2", Role.IDENTIFIER, String.class,
                    "ME1", Role.MEASURE, Integer.class
            ));
            expectedThrowable = null;
            try {
                new SumOperation(tuple -> null, left.getDataStructure(), tuple -> null, right.getDataStructure());
            } catch (Throwable t) {
                expectedThrowable = t;
            }
            softly.assertThat(expectedThrowable)
                    .as("SumOperation exception when no common id type")
                    .isNotNull();

            // any order.
            expectedThrowable = null;
            try {
                new SumOperation(tuple -> null, right.getDataStructure(), tuple -> null, left.getDataStructure());
            } catch (Throwable t) {
                expectedThrowable = t;
            }
            softly.assertThat(expectedThrowable)
                    .as("SumOperation exception when no common id type (reversed)")
                    .isNotNull();

        } finally {
            softly.assertAll();
        }
    }

    /**
     * If both ds_1 and ds_2 are Datasets then either they have
     * one or more measures in common, or at least one of them
     * has only a measure.
     * <p>
     * VTL 1.1 line 2501.
     */
    //@Test
    public void testNoCommonMeasure() throws Exception {

        Dataset left = mock(Dataset.class);
        Dataset right = mock(Dataset.class);

        SoftAssertions softly = new SoftAssertions();
        try {
            when(left.getDataStructure()).thenReturn(DataStructure.of(mapper::convertValue,
                    "ID1", Role.IDENTIFIER, String.class,
                    "ID2", Role.IDENTIFIER, String.class,
                    "ME1", Role.MEASURE, Integer.class
            ));
            Throwable expectedThrowable = null;

            // Different measure
            when(right.getDataStructure()).thenReturn(DataStructure.of(mapper::convertValue,
                    "ID1", Role.IDENTIFIER, String.class,
                    "ID2", Role.IDENTIFIER, String.class,
                    "ME1NOTSAMEMEASURE", Role.MEASURE, Integer.class
            ));
            expectedThrowable = null;
            try {
                new SumOperation(tuple -> null, left.getDataStructure(), tuple -> null, right.getDataStructure());
            } catch (Throwable t) {
                expectedThrowable = t;
            }
            softly.assertThat(expectedThrowable)
                    .as("SumOperation exception when no common measure name")
                    .isNotNull()
                    .hasMessageContaining("ME1NOTSAMEMEASURE");

            // any order
            expectedThrowable = null;
            try {
                new SumOperation(tuple -> null, right.getDataStructure(), tuple -> null, left.getDataStructure());
            } catch (Throwable t) {
                expectedThrowable = t;
            }
            softly.assertThat(expectedThrowable)
                    .as("SumOperation exception when no common measure name (reversed)")
                    .isNotNull();

        } finally {
            softly.assertAll();
        }
    }

    /**
     * Test the example 1
     * <p>
     * VTL 1.1 line 2522-2536.
     *
     * @throws Exception
     */
    //@Test()
    public void testSumEx1() throws Exception {

        Dataset left = mock(Dataset.class);
        Dataset right = mock(Dataset.class);

        SoftAssertions softly = new SoftAssertions();
        try {
            when(left.getDataStructure()).thenReturn(DataStructure.of(mapper::convertValue,
                    "TIME", Role.IDENTIFIER, String.class,
                    "GEO", Role.IDENTIFIER, String.class,
                    "POPULATION", Role.MEASURE, Integer.class
            ));

            when(right.getDataStructure()).thenReturn(DataStructure.of(mapper::convertValue,
                    "TIME", Role.IDENTIFIER, String.class,
                    "GEO", Role.IDENTIFIER, String.class,
                    "AGE", Role.IDENTIFIER, String.class,
                    "POPULATION", Role.MEASURE, Integer.class
            ));

            DataStructure ld = left.getDataStructure();
            when(left.get()).thenReturn(
                    Stream.of(
                            tuple(ld.wrap("TIME", "2013"),
                                    ld.wrap("GEO", "Belgium"),
                                    ld.wrap("POPULATION", 5)),
                            tuple(ld.wrap("TIME", "2013"),
                                    ld.wrap("GEO", "Denmark"),
                                    ld.wrap("POPULATION", 2)),
                            tuple(ld.wrap("TIME", "2013"),
                                    ld.wrap("GEO", "France"),
                                    ld.wrap("POPULATION", 3)),
                            tuple(ld.wrap("TIME", "2013"),
                                    ld.wrap("GEO", "Spain"),
                                    ld.wrap("POPULATION", 4))
                    )
            );

            DataStructure rd = right.getDataStructure();
            when(right.get()).thenReturn(
                    Stream.of(
                            tuple(rd.wrap("TIME", "2013"),
                                    rd.wrap("GEO", "Belgium"),
                                    rd.wrap("AGE", "Total"),
                                    rd.wrap("POPULATION", 10)),
                            tuple(rd.wrap("TIME", "2013"),
                                    rd.wrap("GEO", "Greece"),
                                    rd.wrap("AGE", "Total"),
                                    rd.wrap("POPULATION", 11)),
                            tuple(rd.wrap("TIME", "2013"),
                                    rd.wrap("GEO", "Belgium"),
                                    rd.wrap("AGE", "Y15-24"),
                                    rd.wrap("POPULATION", null)),
                            tuple(rd.wrap("TIME", "2013"),
                                    rd.wrap("GEO", "Greece"),
                                    rd.wrap("AGE", "Y15-24"),
                                    rd.wrap("POPULATION", 2)),
                            tuple(rd.wrap("TIME", "2013"),
                                    rd.wrap("GEO", "Spain"),
                                    rd.wrap("AGE", "Y15-24"),
                                    rd.wrap("POPULATION", 6))
                    )
            );

            InnerJoinOperation join = new InnerJoinOperation(ImmutableMap.of(
                    "left", left, "right", right
            ));
            SumOperation sumOperation = new SumOperation(
                    tuple -> tuple.get(3), ld,
                    tuple -> tuple.get(3), rd
            );
            join.getClauses().add(new JoinClause() {


                @Override
                public WorkingDataset apply(WorkingDataset workingDataset) {
                    return new WorkingDataset() {
                        @Override
                        public DataStructure getDataStructure() {
                            return sumOperation.getDataStructure();
                        }

                        @Override
                        public Stream<Tuple> get() {
                            return workingDataset.get().map(tuple -> sumOperation.apply(tuple, null));

                        }
                    };
                }
            });

            softly.assertThat(
                    join.getDataStructure()
            ).as("data structure of the sum operation of %s and %s", left, right)
                    .isNotNull();
            // TODO: Better check.

            DataStructure sumDs = sumOperation.getDataStructureOperator().apply(
                    left.getDataStructure(), right.getDataStructure()
            );
            softly.assertThat(
                    StreamUtils.zip(
                            left.get(), right.get(), sumOperation.getTupleOperator()
                    )
            ).as("data tuple of the sum operation of %s and %s", left, right)
                    .containsExactly(
                            tuple(sumDs.wrap("TIME", "2013"),
                                    sumDs.wrap("GEO", "Belgium"),
                                    sumDs.wrap("AGE", "Total"),
                                    sumDs.wrap("POPULATION", 15)),
                            tuple(sumDs.wrap("TIME", "2013"),
                                    sumDs.wrap("GEO", "Belgium"),
                                    sumDs.wrap("AGE", "Y15-24"),
                                    sumDs.wrap("POPULATION", null)),
                            tuple(sumDs.wrap("TIME", "2013"),
                                    sumDs.wrap("GEO", "Spain"),
                                    sumDs.wrap("AGE", "Y15-24"),
                                    sumDs.wrap("POPULATION", 10))
                    );

        } finally {
            softly.assertAll();
        }
    }

    /**
     * Test the example 2
     * <p>
     * VTL 1.1 line 2537-2541.
     *
     * @throws Exception
     */
    //@Test
    public void testSumEx2() throws Exception {

        Dataset left = mock(Dataset.class);

        SoftAssertions softly = new SoftAssertions();
        try {
            when(left.getDataStructure()).thenReturn(DataStructure.of(mapper::convertValue,
                    "TIME", Role.IDENTIFIER, String.class,
                    "REF_AREA", Role.IDENTIFIER, String.class,
                    "PARTNER", Role.IDENTIFIER, String.class,
                    "OBS_VALUE", Role.MEASURE, String.class,
                    "OBS_STATUS", Role.ATTRIBUTE, String.class
            ));

            DataStructure ld = left.getDataStructure();
            when(left.get()).thenReturn(
                    Stream.of(
                            tuple(ld.wrap("TIME", "2010"),
                                    ld.wrap("REF_AREA", "EU25"),
                                    ld.wrap("PARTNER", "CA"),
                                    ld.wrap("OBS_VALUE", 20),
                                    ld.wrap("OBS_STATUS", "D")),
                            tuple(ld.wrap("TIME", "2010"),
                                    ld.wrap("REF_AREA", "BG"),
                                    ld.wrap("PARTNER", "CA"),
                                    ld.wrap("OBS_VALUE", 2),
                                    ld.wrap("OBS_STATUS", "D")),
                            tuple(ld.wrap("TIME", "2010"),
                                    ld.wrap("REF_AREA", "RO"),
                                    ld.wrap("PARTNER", "CA"),
                                    ld.wrap("OBS_VALUE", 2),
                                    ld.wrap("OBS_STATUS", "D"))
                    )
            );

            InnerJoinOperation join = new InnerJoinOperation(ImmutableMap.of(
                    "left", left
            ));
            SumOperation sumOperation = new SumOperation(tuple -> tuple.get(3), ld, 1);

            softly.assertThat(
                    join.getDataStructure()
            ).as("data structure of the sum operation of %s and 1", left)
                    .isEqualTo(join.getDataStructure());

            DataStructure sumDs = sumOperation.getDataStructure();
            softly.assertThat(
                    join.get()
            ).as("data of the sum operation of %s and 1", left)
                    .containsExactly(
                            tuple(sumDs.wrap("TIME", "2010"),
                                    sumDs.wrap("REF_AREA", "EU25"),
                                    sumDs.wrap("PARTNER", "CA"),
                                    sumDs.wrap("OBS_VALUE", 21),
                                    sumDs.wrap("OBS_STATUS", "D")),
                            tuple(ld.wrap("TIME", "2010"),
                                    sumDs.wrap("REF_AREA", "BG"),
                                    sumDs.wrap("PARTNER", "CA"),
                                    sumDs.wrap("OBS_VALUE", 3),
                                    sumDs.wrap("OBS_STATUS", "D")),
                            tuple(ld.wrap("TIME", "2010"),
                                    sumDs.wrap("REF_AREA", "RO"),
                                    sumDs.wrap("PARTNER", "CA"),
                                    sumDs.wrap("OBS_VALUE", 3),
                                    sumDs.wrap("OBS_STATUS", "D"))
                    );

        } finally {
            softly.assertAll();
        }
    }

    /**
     * Test the example 2
     * <p>
     * VTL 1.1 line 2537-2541.
     *
     * @throws Exception
     */
    //@Test
    public void testSumEx3() throws Exception {
        Dataset left = mock(Dataset.class);
        Dataset right = mock(Dataset.class);

        SoftAssertions softly = new SoftAssertions();
        try {

            DataStructure ds = DataStructure.of(mapper::convertValue,
                    "TIME", Role.IDENTIFIER, String.class,
                    "REF_AREA", Role.IDENTIFIER, String.class,
                    "PARTNER", Role.IDENTIFIER, String.class,
                    "OBS_VALUE", Role.MEASURE, Integer.class,
                    "OBS_STATUS", Role.ATTRIBUTE, String.class
            );

            // Same DS.
            when(left.getDataStructure()).thenReturn(ds);
            when(right.getDataStructure()).thenReturn(ds);

            DataStructure ld = left.getDataStructure();
            when(left.get()).thenReturn(
                    Stream.of(
                            tuple(ld.wrap("TIME", "2010"),
                                    ld.wrap("REF_AREA", "EU25"),
                                    ld.wrap("PARTNER", "CA"),
                                    ld.wrap("OBS_VALUE", 20),
                                    ld.wrap("OBS_STATUS", "D")),
                            tuple(ld.wrap("TIME", "2010"),
                                    ld.wrap("REF_AREA", "BG"),
                                    ld.wrap("PARTNER", "CA"),
                                    ld.wrap("OBS_VALUE", 2),
                                    ld.wrap("OBS_STATUS", "D")),
                            tuple(ld.wrap("TIME", "2010"),
                                    ld.wrap("REF_AREA", "RO"),
                                    ld.wrap("PARTNER", "CA"),
                                    ld.wrap("OBS_VALUE", 2),
                                    ld.wrap("OBS_STATUS", "D"))
                    )
            );

            when(right.get()).thenReturn(
                    Stream.of(
                            tuple(ld.wrap("TIME", "2010"),
                                    ld.wrap("REF_AREA", "EU25"),
                                    ld.wrap("PARTNER", "CA"),
                                    ld.wrap("OBS_VALUE", 10),
                                    ld.wrap("OBS_STATUS", "D")),
                            tuple(ld.wrap("TIME", "2010"))
                    )
            );

            InnerJoinOperation join = new InnerJoinOperation(ImmutableMap.of(
                    "left", left,
                    "right", right
            ));


            SumOperation sumOperation = new SumOperation(
                    tuple -> tuple.get(3),
                    ld,
                    tuple -> tuple.get(3),
                    ld
            );
            join.getClauses().add(new JoinClause() {
                @Override
                public WorkingDataset apply(WorkingDataset workingDataset) {
                    return new WorkingDataset() {
                        @Override
                        public DataStructure getDataStructure() {
                            return workingDataset.getDataStructure();
                        }

                        @Override
                        public Stream<Tuple> get() {
                            return workingDataset.get().map(tuple -> sumOperation.apply(tuple, null));
                        }

                    };
                }
            });

            softly.assertThat(
                    sumOperation.getDataStructure()
            ).as("data structure of the sum operation of %s and %s", left, right)
                    .isNotEqualTo(left.getDataStructure());

            DataStructure sumDs = join.getDataStructure();
            softly.assertThat(
                    join.get()
            ).as("data of the sum operation of %s and %s", left, right)
                    .containsExactly(
                            tuple(sumDs.wrap("TIME", "2010"),
                                    sumDs.wrap("REF_AREA", "EU25"),
                                    sumDs.wrap("PARTNER", "CA"),
                                    sumDs.wrap("OBS_VALUE", 30))
                    );

        } finally {
            softly.assertAll();
        }
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
