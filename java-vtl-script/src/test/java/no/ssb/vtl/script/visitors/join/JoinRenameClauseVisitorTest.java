package no.ssb.vtl.script.join;

import no.ssb.vtl.model.DataStructure;
import no.ssb.vtl.parser.VTLLexer;
import no.ssb.vtl.parser.VTLParser;
import no.ssb.vtl.script.operations.RenameOperation;
import org.antlr.v4.runtime.*;
import org.junit.Test;

import java.util.stream.Stream;

import static no.ssb.vtl.model.Component.Role;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class JoinRenameClauseVisitorTest {

    @Test
    public void testRename() throws Exception {

        WorkingDataset dataset = mock(WorkingDataset.class);
        DataStructure structure = DataStructure.of((o, aClass) -> o,
                "foo.identifier", Role.IDENTIFIER, String.class,
                "foo.measure", Role.MEASURE, String.class,
                "foo.attribute", Role.ATTRIBUTE, String.class,
                "identifier", Role.IDENTIFIER, String.class,
                "measure", Role.MEASURE, String.class,
                "attribute", Role.ATTRIBUTE, String.class
        );
        when(dataset.getDataStructure()).thenReturn(structure);
        when(dataset.get()).thenReturn(Stream.empty());


        String test = "rename" +
                " foo.identifier to renamedFooIdentifier," +
                " foo.measure to renamedFooMeasure," +
                " foo.attribute to renamedFooAttribute," +
                " identifier to renamedIdentifier," +
                " measure to renamedMeasure, " +
                " attribute to renamedAttribute";
        VTLLexer lexer = new VTLLexer(new ANTLRInputStream(test));
        VTLParser parser = new VTLParser(new CommonTokenStream(lexer));

        lexer.addErrorListener(new ConsoleErrorListener());
        lexer.addErrorListener(new BaseErrorListener() {
            @Override
            public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine, String msg, RecognitionException e) {
                throw new RuntimeException(msg, e);
            }
        });
        parser.addErrorListener(new ConsoleErrorListener());
        parser.addErrorListener(new BaseErrorListener() {
            @Override
            public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine, String msg, RecognitionException e) {
                throw new RuntimeException(msg, e);
            }
        });

        JoinRenameClauseVisitor visitor = new JoinRenameClauseVisitor(dataset);

        RenameOperation visit = visitor.visit(parser.joinRenameExpression());

        assertThat(visit.getDataStructure()).containsOnlyKeys(
                "renamedFooIdentifier", "renamedFooMeasure", "renamedFooAttribute",
                "renamedIdentifier", "renamedMeasure", "renamedAttribute"
        );

    }
}
