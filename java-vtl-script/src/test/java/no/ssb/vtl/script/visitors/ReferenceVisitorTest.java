package no.ssb.vtl.script.visitors;


import com.google.common.collect.ImmutableMap;
import no.ssb.vtl.model.Component;
import no.ssb.vtl.model.DataStructure;
import no.ssb.vtl.model.Dataset;
import no.ssb.vtl.parser.VTLLexer;
import no.ssb.vtl.parser.VTLParser;
import no.ssb.vtl.script.error.SyntaxException;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.BailErrorStrategy;
import org.antlr.v4.runtime.CommonTokenStream;
import org.assertj.core.api.AutoCloseableSoftAssertions;
import org.assertj.core.util.Maps;
import org.junit.Before;
import org.junit.Test;

import javax.script.Bindings;
import javax.script.SimpleBindings;

import static no.ssb.vtl.model.Component.Role.*;
import static org.mockito.Mockito.*;

public class ReferenceVisitorTest {

    private SimpleBindings bindings;
    private Component component;
    private Dataset dataset;

    private static VTLParser parse(String expression) {
        VTLLexer lexer = new VTLLexer(new ANTLRInputStream(expression));
        VTLParser parser = new VTLParser(new CommonTokenStream(lexer));
        parser.setErrorHandler(new BailErrorStrategy());
        return parser;
    }

    @Before
    public void setUp() throws Exception {
        DataStructure structure = DataStructure.of((o, aClass) -> o,
                "component", IDENTIFIER, String.class
        );
        component = structure.get("component");
        dataset = mock(Dataset.class);
        bindings = new SimpleBindings(ImmutableMap.of(
                "component", component,
                "dataset", dataset
        ));

        when(dataset.getDataStructure()).thenReturn(structure);
    }

    @Test
    public void testNotFound() throws Exception {
        Bindings emptyBindings = new SimpleBindings(
                Maps.newHashMap("dataset", dataset));
        ReferenceVisitor referenceVisitor = new ReferenceVisitor(emptyBindings);

        try (AutoCloseableSoftAssertions softly = new AutoCloseableSoftAssertions()) {
            softly.assertThatThrownBy(() -> referenceVisitor.visit(parse("componentNotFound").componentRef()))
                    .describedAs("exception when component not found")
                    .hasCauseInstanceOf(SyntaxException.class)
                    .hasStackTraceContaining("variable")
                    .hasStackTraceContaining("componentNotFound")
                    .hasStackTraceContaining("not found");

            softly.assertThatThrownBy(() -> referenceVisitor.visit(parse("dataset.componentNotFound").componentRef()))
                    .describedAs("exception when component not found")
                    .hasCauseInstanceOf(SyntaxException.class)
                    .hasStackTraceContaining("variable")
                    .hasStackTraceContaining("componentNotFound")
                    .hasStackTraceContaining("not found");

            softly.assertThatThrownBy(() -> referenceVisitor.visit(parse("datasetNotFound").datasetRef()))
                    .describedAs("exception when component not found")
                    .hasCauseInstanceOf(SyntaxException.class)
                    .hasStackTraceContaining("variable")
                    .hasStackTraceContaining("datasetNotFound")
                    .hasStackTraceContaining("not found");

            softly.assertThatThrownBy(() -> referenceVisitor.visit(parse("variableNotFound").variableRef()))
                    .describedAs("exception when component not found")
                    .hasCauseInstanceOf(SyntaxException.class)
                    .hasStackTraceContaining("variable")
                    .hasStackTraceContaining("variableNotFound")
                    .hasStackTraceContaining("not found");

        }
    }

    @Test
    public void testComponents() throws Exception {
        String[] components = new String[]{
                "component",
                "'component'",
                "dataset.'component'",
                "'dataset'.component"
        };

        try (AutoCloseableSoftAssertions softly = new AutoCloseableSoftAssertions()) {
            ReferenceVisitor referenceVisitor = new ReferenceVisitor(bindings);
            for (String componentExpression : components) {
                VTLParser parser = parse(componentExpression);
                softly.assertThat(referenceVisitor.visit(parser.componentRef()))
                        .describedAs("resolved componentRef for [%s]", componentExpression)
                        .isSameAs(component);
            }
        }
    }

    @Test
    public void testDatasets() throws Exception {
        String[] datasets = new String[]{
                "dataset",
                "'dataset'",
        };

        try (AutoCloseableSoftAssertions softly = new AutoCloseableSoftAssertions()) {
            ReferenceVisitor referenceVisitor = new ReferenceVisitor(bindings);
            for (String datasetExpression : datasets) {
                VTLParser parser = parse(datasetExpression);
                softly.assertThat(referenceVisitor.visit(parser.datasetRef()))
                        .describedAs("resolved datasetRef for [%s]", datasetExpression)
                        .isNotNull()
                        .isSameAs(dataset);
            }
        }
    }
}
