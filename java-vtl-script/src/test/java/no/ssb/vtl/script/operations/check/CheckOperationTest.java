package no.ssb.vtl.script.operations.check;

import no.ssb.vtl.model.Component;
import no.ssb.vtl.model.DataPoint;
import no.ssb.vtl.model.DataStructure;
import no.ssb.vtl.model.Dataset;
import no.ssb.vtl.script.operations.CheckOperation;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static java.util.stream.Collectors.*;
import static org.assertj.core.api.Assertions.*;
import static org.hamcrest.Matchers.*;
import static org.mockito.Mockito.*;

public class CheckOperationTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testArgumentDataset() throws Exception {
        thrown.expect(NullPointerException.class);
        thrown.expect(hasProperty("message", containsString("dataset was null")));
        new CheckOperation(null, null, null, null, null);
    }

    /**
     * NOTE: This is not in the spec for check with single rule, but exists for check with datapoint rulesets.
     * <p>
     * VTL 1.1, line 4799.
     */
    @Test
    public void testArgumentAllAndMeasuresToReturn() throws Exception {
        thrown.expect(IllegalArgumentException.class);
        thrown.expect(hasProperty("message", containsString("cannot use 'all' with 'measures'")));
        new CheckOperation(mock(Dataset.class), Optional.of(CheckOperation.RowsToReturn.ALL),
                Optional.of(CheckOperation.ComponentsToReturn.MEASURES), null, null);
    }

    /**
     * The input Dataset must have all Boolean Measure Components.
     * NOTE: The spec is not consistent with regard to parameters and constraints sections (line 4925-4926 and 4949).
     * We decided to allow more than one Boolean Measure Component.
     * <p>
     * VTL 1.1 line 4949.
     */
    @Test
    public void testArgumentDatasetComponentsTooManyBooleans() throws Exception {
        Dataset dataset = mock(Dataset.class);
        when(dataset.getDataStructure()).thenReturn(
                DataStructure.of((s, o) -> null,
                        "id1", Component.Role.IDENTIFIER, String.class,
                        "me1", Component.Role.MEASURE, Boolean.class,
                        "me2", Component.Role.MEASURE, Boolean.class,
                        "at1", Component.Role.ATTRIBUTE, String.class
                )
        );

        new CheckOperation(dataset, Optional.of(CheckOperation.RowsToReturn.VALID),
                Optional.of(CheckOperation.ComponentsToReturn.MEASURES), null, null);
    }

    @Test
    public void testCheckReturnMeasuresNotValidRows() throws Exception {
        //This data structure is a result of a boolean operation, so it will either has one CONDITION component
        //or more with "_CONDITION" suffix for each component.
        DataStructure dataStructure = DataStructure.of((s, o) -> s,
                "kommune_nr", Component.Role.IDENTIFIER, String.class,
                "code", Component.Role.IDENTIFIER, String.class, //from KLASS
                "CONDITION", Component.Role.MEASURE, Boolean.class
        );

        Dataset ds = mock(Dataset.class);
        when(ds.getDataStructure()).thenReturn(dataStructure);

        when(ds.get()).thenReturn(Stream.of(
                tuple(
                        dataStructure.wrap("kommune_nr", "0101"),
                        dataStructure.wrap("code", "0101"),
                        dataStructure.wrap("CONDITION", true)
                ), tuple(
                        dataStructure.wrap("kommune_nr", "9990"),
                        dataStructure.wrap("code", null), //not in the code list, so a null value
                        dataStructure.wrap("CONDITION", false)
                ), tuple(
                        dataStructure.wrap("kommune_nr", "0104"),
                        dataStructure.wrap("code", "0104"),
                        dataStructure.wrap("CONDITION", true)
                )
        ));

        CheckOperation checkOperation = new CheckOperation(ds, Optional.of(CheckOperation.RowsToReturn.NOT_VALID),
                Optional.of(CheckOperation.ComponentsToReturn.MEASURES), null, null);

        assertThat(checkOperation.getDataStructure().getRoles()).contains(
                entry("kommune_nr", Component.Role.IDENTIFIER),
                entry("code", Component.Role.IDENTIFIER),
                entry("CONDITION", Component.Role.MEASURE),
                entry("errorcode", Component.Role.ATTRIBUTE),
                entry("errorlevel", Component.Role.ATTRIBUTE)
        );

        Stream<Dataset.Tuple> stream = checkOperation.stream();
        assertThat(stream).isNotNull();

        List<Dataset.Tuple> collect = stream.collect(toList());
        assertThat(collect).
                extractingResultOf("ids").isNotEmpty().hasSize(1);

        assertThat(getDataPointsWithComponentName(collect, "kommune_nr")).extracting(DataPoint::get).containsOnlyOnce("9990");
        assertThat(getDataPointsWithComponentName(collect, "code")).extracting(DataPoint::get).containsNull();
        assertThat(getDataPointsWithComponentName(collect, "CONDITION")).extracting(DataPoint::get).containsOnly(false);
        assertThat(getDataPointsWithComponentName(collect, "errorlevel")).extracting(DataPoint::get).containsNull();
        assertThat(getDataPointsWithComponentName(collect, "errorcode")).extracting(DataPoint::get).containsNull();
    }

    @Test
    public void testCheckReturnMeasuresValidRows() throws Exception {
        //This data structure is a result of a boolean operation, so it will either has one CONDITION component
        //or more with "_CONDITION" suffix for each component.
        DataStructure dataStructure = DataStructure.of((s, o) -> s,
                "kommune_nr", Component.Role.IDENTIFIER, String.class,
                "code", Component.Role.IDENTIFIER, String.class, //from KLASS
                "CONDITION", Component.Role.MEASURE, Boolean.class
        );

        Dataset ds = mock(Dataset.class);
        when(ds.getDataStructure()).thenReturn(dataStructure);

        when(ds.get()).thenReturn(Stream.of(
                tuple(
                        dataStructure.wrap("kommune_nr", "0101"),
                        dataStructure.wrap("code", "0101"),
                        dataStructure.wrap("CONDITION", true)
                ), tuple(
                        dataStructure.wrap("kommune_nr", "9990"),
                        dataStructure.wrap("code", null), //not in the code list, so a null value
                        dataStructure.wrap("CONDITION", false)
                ), tuple(
                        dataStructure.wrap("kommune_nr", "0104"),
                        dataStructure.wrap("code", "0104"),
                        dataStructure.wrap("CONDITION", true)
                )
        ));

        CheckOperation checkOperation = new CheckOperation(ds, Optional.of(CheckOperation.RowsToReturn.VALID),
                Optional.of(CheckOperation.ComponentsToReturn.MEASURES), null, null);

        assertThat(checkOperation.getDataStructure().getRoles()).contains(
                entry("kommune_nr", Component.Role.IDENTIFIER),
                entry("code", Component.Role.IDENTIFIER),
                entry("CONDITION", Component.Role.MEASURE),
                entry("errorcode", Component.Role.ATTRIBUTE),
                entry("errorlevel", Component.Role.ATTRIBUTE)
        );

        Stream<Dataset.Tuple> stream = checkOperation.stream();
        assertThat(stream).isNotNull();

        List<Dataset.Tuple> collect = stream.collect(toList());
        assertThat(collect).
                extractingResultOf("ids").isNotEmpty().hasSize(2);

        assertThat(getDataPointsWithComponentName(collect, "kommune_nr")).extracting(DataPoint::get).containsOnlyOnce("0101", "0101");
        assertThat(getDataPointsWithComponentName(collect, "code")).extracting(DataPoint::get).containsOnlyOnce("0101", "0101");
        assertThat(getDataPointsWithComponentName(collect, "CONDITION")).extracting(DataPoint::get).containsOnly(true);
        assertThat(getDataPointsWithComponentName(collect, "errorlevel")).extracting(DataPoint::get).containsNull();
        assertThat(getDataPointsWithComponentName(collect, "errorcode")).extracting(DataPoint::get).containsNull();
    }

    @Test
    public void testCheckReturnConditionNotValidRows() throws Exception {
        //This data structure is a result of a boolean operation, so it will either has one CONDITION component
        //or more with "_CONDITION" suffix for each component.
        //No attribute components as the VTL 1.1 does not specify that.
        DataStructure dataStructure = DataStructure.of((s, o) -> s,
                "kommune_nr", Component.Role.IDENTIFIER, String.class,
                "code", Component.Role.IDENTIFIER, String.class, //from KLASS
                "CONDITION_CONDITION", Component.Role.MEASURE, Boolean.class,
                "booleanMeasure_CONDITION", Component.Role.MEASURE, Boolean.class,
                "stringMeasure", Component.Role.MEASURE, String.class
        );

        Dataset ds = mock(Dataset.class);
        when(ds.getDataStructure()).thenReturn(dataStructure);

        when(ds.get()).thenReturn(Stream.of(
                tuple(
                        dataStructure.wrap("kommune_nr", "0101"),
                        dataStructure.wrap("code", "0101"),
                        dataStructure.wrap("CONDITION_CONDITION", true),
                        dataStructure.wrap("booleanMeasure_CONDITION", true),
                        dataStructure.wrap("stringMeasure", "t1")
                ), tuple(
                        dataStructure.wrap("kommune_nr", "9990"),
                        dataStructure.wrap("code", null),
                        dataStructure.wrap("CONDITION_CONDITION", true),
                        dataStructure.wrap("booleanMeasure_CONDITION", false),
                        dataStructure.wrap("stringMeasure", "t2")
                ), tuple(
                        dataStructure.wrap("kommune_nr", "0104"),
                        dataStructure.wrap("code", null), //not in the code list, so a null value
                        dataStructure.wrap("CONDITION_CONDITION", false),
                        dataStructure.wrap("booleanMeasure_CONDITION", false),
                        dataStructure.wrap("stringMeasure", "t3")
                )
        ));

        CheckOperation checkOperation = new CheckOperation(ds, Optional.of(CheckOperation.RowsToReturn.NOT_VALID),
                Optional.of(CheckOperation.ComponentsToReturn.CONDITION), null, null);

        assertThat(checkOperation.getDataStructure().getRoles()).contains(
                entry("kommune_nr", Component.Role.IDENTIFIER),
                entry("code", Component.Role.IDENTIFIER),
                entry("CONDITION_CONDITION", Component.Role.MEASURE),
                entry("booleanMeasure_CONDITION", Component.Role.MEASURE),
                entry("CONDITION", Component.Role.MEASURE),   //new component, result of CONDITION_CONDITION && booleanMeasure_CONDITION
                entry("errorcode", Component.Role.ATTRIBUTE), //new component
                entry("errorlevel", Component.Role.ATTRIBUTE) //new component
        );

        Stream<Dataset.Tuple> stream = checkOperation.stream();
        assertThat(stream).isNotNull();

        List<Dataset.Tuple> collect = stream.collect(toList());
        assertThat(collect).
                extractingResultOf("ids").isNotEmpty().hasSize(2);

        assertThat(getDataPointsWithComponentName(collect, "kommune_nr")).extracting(DataPoint::get).containsOnlyOnce("9990", "0104");
        assertThat(getDataPointsWithComponentName(collect, "code")).extracting(DataPoint::get).containsNull();
        assertThat(getDataPointsWithComponentName(collect, "CONDITION_CONDITION")).extracting(DataPoint::get).containsExactlyInAnyOrder(true, false);
        assertThat(getDataPointsWithComponentName(collect, "booleanMeasure_CONDITION")).extracting(DataPoint::get).containsOnly(false);
        assertThat(getDataPointsWithComponentName(collect, "CONDITION")).extracting(DataPoint::get).containsOnly(false);
        assertThat(getDataPointsWithComponentName(collect, "errorlevel")).extracting(DataPoint::get).containsNull();
        assertThat(getDataPointsWithComponentName(collect, "errorcode")).extracting(DataPoint::get).containsNull();
    }

    @Test
    public void testCheckReturnConditionValidRows() throws Exception {
        //This data structure is a result of a boolean operation, so it will either has one CONDITION component
        //or more with "_CONDITION" suffix for each component.
        //No attribute components as the VTL 1.1 does not specify that.
        DataStructure dataStructure = DataStructure.of((s, o) -> s,
                "kommune_nr", Component.Role.IDENTIFIER, String.class,
                "code", Component.Role.IDENTIFIER, String.class, //from KLASS
                "CONDITION_CONDITION", Component.Role.MEASURE, Boolean.class,
                "booleanMeasure_CONDITION", Component.Role.MEASURE, Boolean.class,
                "stringMeasure", Component.Role.MEASURE, String.class
        );

        Dataset ds = mock(Dataset.class);
        when(ds.getDataStructure()).thenReturn(dataStructure);

        when(ds.get()).thenReturn(Stream.of(
                tuple(
                        dataStructure.wrap("kommune_nr", "0101"),
                        dataStructure.wrap("code", "0101"),
                        dataStructure.wrap("CONDITION_CONDITION", true),
                        dataStructure.wrap("booleanMeasure_CONDITION", true),
                        dataStructure.wrap("stringMeasure", "t1")
                ), tuple(
                        dataStructure.wrap("kommune_nr", "9990"),
                        dataStructure.wrap("code", null),
                        dataStructure.wrap("CONDITION_CONDITION", true),
                        dataStructure.wrap("booleanMeasure_CONDITION", false),
                        dataStructure.wrap("stringMeasure", "t2")
                ), tuple(
                        dataStructure.wrap("kommune_nr", "0104"),
                        dataStructure.wrap("code", null), //not in the code list, so a null value
                        dataStructure.wrap("CONDITION_CONDITION", false),
                        dataStructure.wrap("booleanMeasure_CONDITION", false),
                        dataStructure.wrap("stringMeasure", "t3")
                )
        ));

        CheckOperation checkOperation = new CheckOperation(ds, Optional.of(CheckOperation.RowsToReturn.VALID),
                Optional.of(CheckOperation.ComponentsToReturn.CONDITION), null, null);

        assertThat(checkOperation.getDataStructure().getRoles()).contains(
                entry("kommune_nr", Component.Role.IDENTIFIER),
                entry("code", Component.Role.IDENTIFIER),
                entry("CONDITION_CONDITION", Component.Role.MEASURE),
                entry("booleanMeasure_CONDITION", Component.Role.MEASURE),
                entry("CONDITION", Component.Role.MEASURE),   //new component, result of CONDITION_CONDITION && booleanMeasure_CONDITION
                entry("errorcode", Component.Role.ATTRIBUTE), //new component
                entry("errorlevel", Component.Role.ATTRIBUTE) //new component
        );

        Stream<Dataset.Tuple> stream = checkOperation.stream();
        assertThat(stream).isNotNull();

        List<Dataset.Tuple> collect = stream.collect(toList());
        assertThat(collect).
                extractingResultOf("ids").isNotEmpty().hasSize(1);

        assertThat(getDataPointsWithComponentName(collect, "kommune_nr")).extracting(DataPoint::get).containsOnlyOnce("0101");
        assertThat(getDataPointsWithComponentName(collect, "code")).extracting(DataPoint::get).containsOnlyOnce("0101");
        assertThat(getDataPointsWithComponentName(collect, "CONDITION_CONDITION")).extracting(DataPoint::get).containsOnly(true);
        assertThat(getDataPointsWithComponentName(collect, "booleanMeasure_CONDITION")).extracting(DataPoint::get).containsOnly(true);
        assertThat(getDataPointsWithComponentName(collect, "CONDITION")).extracting(DataPoint::get).containsOnly(true);
        assertThat(getDataPointsWithComponentName(collect, "errorlevel")).extracting(DataPoint::get).containsNull();
        assertThat(getDataPointsWithComponentName(collect, "errorcode")).extracting(DataPoint::get).containsNull();
    }

    private ArrayList<DataPoint> getDataPointsWithComponentName(List<Dataset.Tuple> tuple, String componentName) {
        ArrayList<DataPoint> dpsFound = new ArrayList<>();

        for (Dataset.Tuple dataPoints : tuple) {
            List<DataPoint> dpListOfOne = dataPoints.stream()
                    .filter(dp -> dp.getName().equals(componentName))
                    .collect(toList());
            if (dpListOfOne.size() == 1) {
                dpsFound.add(dpListOfOne.get(0));
            } else {
                throw new IllegalArgumentException("Should have found 1 DataPoint object, but found: " + Arrays.toString(dpListOfOne.toArray()));
            }
        }

        return dpsFound;
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
