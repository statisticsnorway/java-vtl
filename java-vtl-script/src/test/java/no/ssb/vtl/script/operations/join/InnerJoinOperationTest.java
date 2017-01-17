package no.ssb.vtl.script.visitors.join;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import no.ssb.vtl.model.DataPoint;
import no.ssb.vtl.model.DataStructure;
import no.ssb.vtl.model.Dataset;
import org.assertj.core.api.Condition;
import org.junit.Test;

import java.time.Year;
import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static no.ssb.vtl.model.Component.Role;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

public class InnerJoinOperationTest {

    private ObjectMapper mapper = new ObjectMapper();

    @Test
    public void testDefaultJoin() throws Exception {
        Dataset ds1 = mock(Dataset.class);
        Dataset ds2 = mock(Dataset.class);

        DataStructure structure1 = DataStructure.of(
                mapper::convertValue,
                "time", Role.IDENTIFIER, Year.class,
                "ref_area", Role.IDENTIFIER, String.class,
                "partner", Role.IDENTIFIER, String.class,
                "obs_value", Role.MEASURE, Integer.class,
                "obs_status", Role.ATTRIBUTE, String.class
        );

        DataStructure structure2 = DataStructure.of(
                mapper::convertValue,
                "time", Role.IDENTIFIER, Year.class,
                "ref_area", Role.IDENTIFIER, String.class,
                "partner", Role.IDENTIFIER, String.class,
                "obs_value", Role.MEASURE, Integer.class,
                "obs_status", Role.ATTRIBUTE, String.class
        );

        given(ds1.getDataStructure()).willReturn(structure1);
        given(ds2.getDataStructure()).willReturn(structure2);

        given(ds1.get()).willReturn(Stream.of(
                tuple(
                        structure1.wrap("time", Year.of(2010)),
                        structure1.wrap("ref_area", "EU25"),
                        structure1.wrap("partner", "CA"),
                        structure1.wrap("obs_value", "20"),
                        structure1.wrap("obs_status", "E")
                ), tuple(
                        structure1.wrap("time", Year.of(2010)),
                        structure1.wrap("ref_area", "EU25"),
                        structure1.wrap("partner", "BG"),
                        structure1.wrap("obs_value", "2"),
                        structure1.wrap("obs_status", "P")
                ), tuple(
                        structure1.wrap("time", Year.of(2010)),
                        structure1.wrap("ref_area", "EU25"),
                        structure1.wrap("partner", "RO"),
                        structure1.wrap("obs_value", "2"),
                        structure1.wrap("obs_status", "P")
                )
        ));

        given(ds2.get()).willReturn(Stream.of(
                tuple(
                        structure2.wrap("time", Year.of(2010)),
                        structure2.wrap("ref_area", "EU25"),
                        structure2.wrap("partner", "CA"),
                        structure2.wrap("obs_value", "10"),
                        structure2.wrap("obs_status", "P")
                )
        ));

        AbstractJoinOperation result = new InnerJoinOperation(ImmutableMap.of("ds1", ds1, "ds2", ds2));

        /*result.getClauses().add(new JoinClause() {
            @Override
            public DataStructure transformDataStructure(DataStructure structure1) {
                return structure1;
            }

            @Override
            public Dataset.Tuple transformTuple(Dataset.Tuple structure1) {
                return structure1;
            }
        });*/

//        assertThat(result.get())
//                .have(dataPointWith("time", Year.of(2010)))
//                .have(dataPointWith("ref_area", "EU25"))
//                .have(dataPointWith("partner", "CA"))
//                .have(dataPointWith("ds1.obs_value", "20"))
//                .have(dataPointWith("ds1.obs_status", "E"))
//                .have(dataPointWith("ds2.obs_value", "10"))
//                .have(dataPointWith("ds2.obs_status", "P"))
//                .have(dataPointWith("time", Year.of(2010)));
    }

    private static Condition<DataPoint> dataPointWith(String name, Object value) {
        return new Condition<DataPoint>(new Predicate<DataPoint>() {
            @Override
            public boolean test(DataPoint dataPoint) {
                return name.equals(dataPoint.getName()) &&
                            value.equals(dataPoint.get());
            }
        }, "data point with name %s and value %s", name, value);
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
