package kohl.hadrien.vtl.interpreter;

import com.google.common.collect.Lists;
import kohl.hadrien.vtl.parser.VTLLexer;
import kohl.hadrien.vtl.parser.VTLParser;
import org.antlr.v4.runtime.*;
import org.fusesource.jansi.Ansi;

import java.io.IOException;
import java.io.LineNumberReader;
import java.io.StringReader;
import java.util.List;

import static org.fusesource.jansi.Ansi.ansi;

public class VTLValidator {

    private final VTLLexer lexer;
    private final VTLParser parser;
    private final SyntaxErrorReporter reporter;
    private String error = "";
    private List<SyntaxError> syntaxErrors = Lists.newArrayList();

    public VTLValidator() {
        reporter = new SyntaxErrorReporter();

        lexer = new VTLLexer(new ANTLRInputStream(""));
        parser = new VTLParser(new CommonTokenStream(lexer));

        lexer.removeErrorListeners();
        parser.removeErrorListeners();

        parser.addErrorListener(reporter);
        lexer.addErrorListener(reporter);

    }

    public boolean isValidStatement(String statement) {
        syntaxErrors.clear();

        lexer.setInputStream(new ANTLRInputStream(statement));
        lexer.reset();
        parser.setInputStream(new CommonTokenStream(lexer));
        parser.reset();
        //parser.addErrorListener(reporter);

        parser.statement();

        if (syntaxErrors.isEmpty())
            return true;

        try {
            StringBuilder result = new StringBuilder();
            LineNumberReader reader = new LineNumberReader(
                    new StringReader(statement)
            );
            Ansi coloredError = ansi();
            String line;
            for (SyntaxError error : syntaxErrors) {
                if (error.line < reader.getLineNumber()) {
                    coloredError.a(reader.readLine());
                } else {
                    line = reader.readLine();

                    if (line == null)
                        break;

                    coloredError.fgDefault().a(line.substring(0, error.pos));

                    if (error.pos < line.length()) {
                        coloredError.fgRed().a(line.substring(error.pos, error.pos + 1));
                        coloredError.fgDefault().a(line.substring(error.pos + 1));
                    }

                    coloredError.a("\n");


                    result.append(
                            String.format("syntax error:(%d,%d) %s.\n", error.line, error.pos, error.message)
                    );
                }
            }
            while ((line = reader.readLine()) != null)
                coloredError.a(line + "\n");
            result.insert(0, coloredError.toString());

            this.error = result.toString();

        } catch (IOException ioe) {
            // Cannot happen.
        }
        return false;
    }

    public String getError() {
        String error = this.error;
        this.error = "";
        return error;
    }

    static class SyntaxError {
        Integer line;
        Integer pos;
        String message;
    }

    private class SyntaxErrorReporter extends BaseErrorListener {

        @Override
        public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine, String msg, RecognitionException e) {
            SyntaxError err = new SyntaxError();
            err.line = line;
            err.pos = charPositionInLine;
            err.message = msg;
            syntaxErrors.add(err);
        }
    }
}
