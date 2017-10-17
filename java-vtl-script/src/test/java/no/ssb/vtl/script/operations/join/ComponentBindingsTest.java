package no.ssb.vtl.script.operations.join;

import com.google.common.collect.ImmutableMap;
import no.ssb.vtl.model.Component;
import no.ssb.vtl.model.Dataset;
import no.ssb.vtl.model.StaticDataset;
import no.ssb.vtl.model.VTLBoolean;
import no.ssb.vtl.model.VTLDate;
import no.ssb.vtl.model.VTLNumber;
import no.ssb.vtl.model.VTLString;
import no.ssb.vtl.model.VTLTyped;
import org.junit.Test;

import javax.script.Bindings;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

public class ComponentBindingsTest {

    @Test
    public void testDatasetBindings() throws Exception {

        Dataset dataset = StaticDataset.create()
                .addComponent("c1", Component.Role.IDENTIFIER, String.class)
                .addComponent("c2", Component.Role.IDENTIFIER, Long.class)
                .addComponent("c3", Component.Role.IDENTIFIER, Double.class)
                .addComponent("c4", Component.Role.IDENTIFIER, Instant.class)
                .addComponent("c5", Component.Role.IDENTIFIER, Boolean.class)
                .build();

        ComponentBindings bindings = new ComponentBindings(dataset);

        assertThat(((VTLTyped<?>) bindings.get("c1")).getVTLType()).isEqualTo(VTLString.class);
        assertThat(((VTLTyped<?>) bindings.get("c2")).getVTLType()).isEqualTo(VTLNumber.class);
        assertThat(((VTLTyped<?>) bindings.get("c3")).getVTLType()).isEqualTo(VTLNumber.class);
        assertThat(((VTLTyped<?>) bindings.get("c4")).getVTLType()).isEqualTo(VTLDate.class);
        assertThat(((VTLTyped<?>) bindings.get("c5")).getVTLType()).isEqualTo(VTLBoolean.class);

    }

    @Test
    // TODO: Move to ComponentBindingsTest
    public void testJoinBindings() throws Exception {

        StaticDataset t1 = StaticDataset.create()
                .addComponent("id1", Component.Role.IDENTIFIER, String.class)
                .addComponent("id2", Component.Role.IDENTIFIER, Long.class)
                .addComponent("uni1", Component.Role.IDENTIFIER, Double.class)
                .addComponent("m1", Component.Role.MEASURE, Instant.class)
                .addComponent("a1", Component.Role.MEASURE, Boolean.class)
                .addComponent("t3", Component.Role.MEASURE, Boolean.class)
                .build();

        StaticDataset t2 = StaticDataset.create()
                .addComponent("id1", Component.Role.IDENTIFIER, String.class)
                .addComponent("id2", Component.Role.IDENTIFIER, Long.class)
                .addComponent("uni2", Component.Role.IDENTIFIER, Double.class)
                .addComponent("uni5", Component.Role.MEASURE, Instant.class)
                .addComponent("a1", Component.Role.MEASURE, Boolean.class)
                .addComponent("t1", Component.Role.MEASURE, Boolean.class)
                .build();

        StaticDataset t3 = StaticDataset.create()
                .addComponent("id1", Component.Role.IDENTIFIER, String.class)
                .addComponent("id2", Component.Role.IDENTIFIER, Long.class)
                .addComponent("uni3", Component.Role.IDENTIFIER, Double.class)
                .addComponent("m1", Component.Role.MEASURE, Instant.class)
                .addComponent("uni4", Component.Role.MEASURE, Boolean.class)
                .addComponent("t2", Component.Role.MEASURE, Boolean.class)
                .build();

        Bindings result = AbstractJoinOperation.createJoinScope(ImmutableMap.of(
                "t1", t1,
                "t2", t2,
                "t3", t3
        ));

        assertThat(result).containsOnlyKeys("uni4", "uni5", "uni2", "uni3", "uni1", "t1", "t2", "t3");

        assertThat(result.get("uni1")).isInstanceOf(VTLTyped.class);
        assertThat(result.get("uni2")).isInstanceOf(VTLTyped.class);
        assertThat(result.get("uni3")).isInstanceOf(VTLTyped.class);
        assertThat(result.get("uni4")).isInstanceOf(VTLTyped.class);
        assertThat(result.get("uni5")).isInstanceOf(VTLTyped.class);

        assertThat(result.get("t1")).isInstanceOf(ComponentBindings.class);
        assertThat(result.get("t2")).isInstanceOf(ComponentBindings.class);
        assertThat(result.get("t3")).isInstanceOf(ComponentBindings.class);
    }
}