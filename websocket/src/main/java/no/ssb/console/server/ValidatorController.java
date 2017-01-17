package kohl.hadrien.console.server;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.Lists;
import kohl.hadrien.vtl.parser.VTLLexer;
import kohl.hadrien.vtl.parser.VTLParser;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.antlr.v4.tool.GrammarParserInterpreter;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.io.Reader;
import java.util.List;

/**
 * Created by hadrien on 08/12/2016.
 */
@RequestMapping(
        produces = MediaType.APPLICATION_JSON_UTF8_VALUE,
        consumes = MediaType.ALL_VALUE
)
@RestController
public class ValidatorController {


    @RequestMapping(
            path = "/validate",
            method = RequestMethod.POST
    )
    public List<Error> validate(Reader script) throws IOException {

        VTLLexer lexer = new VTLLexer(new ANTLRInputStream(script));
        VTLParser parser = new VTLParser(new BufferedTokenStream(lexer));

        lexer.removeErrorListeners();
        parser.removeErrorListeners();

        parser.setErrorHandler(new GrammarParserInterpreter.BailButConsumeErrorStrategy());

        ErrorListener errorListener = new ErrorListener();
        lexer.addErrorListener(errorListener);
        parser.addErrorListener(new DiagnosticErrorListener());
        parser.addErrorListener(errorListener);

        parser.getInterpreter().setPredictionMode(PredictionMode.LL_EXACT_AMBIG_DETECTION);

        parser.start();


        return errorListener.getErrors();

    }

    public static class Error {

        private final Integer startLine;
        private final Integer stopLine;
        private final Integer startColumn;
        private final Integer stopColumn;
        private final String message;
        private final RecognitionException exception;

        public Error(Integer startLine, Integer stopLine, Integer startColumn, Integer stopColumn, String message, RecognitionException exception) {
            this.startLine = startLine;
            this.stopLine = stopLine;
            this.startColumn = startColumn;
            this.stopColumn = stopColumn;
            this.message = message;
            this.exception = exception;
        }

        public Integer getStartLine() {
            return startLine;
        }

        public Integer getStopLine() {
            return stopLine;
        }

        public Integer getStartColumn() {
            return startColumn;
        }

        public Integer getStopColumn() {
            return stopColumn;
        }

        public String getMessage() {
            return message;
        }

        @JsonIgnore
        public RecognitionException getException() {
            return exception;
        }
    }

    public static class ErrorListener extends BaseErrorListener {

        private List<Error> errors = Lists.newArrayList();

        public List<Error> getErrors() {
            return errors;
        }

        @Override
        public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine, String msg, RecognitionException e) {
            int startLine = line, stopLine = line;
            int startColumn = charPositionInLine, stopColumn = charPositionInLine;
            if (offendingSymbol instanceof Token) {
                Token symbol = (Token) offendingSymbol;
                int start = symbol.getStartIndex();
                int stop = symbol.getStopIndex();
                if (start >= 0 && stop >= 0) {
                    stopColumn = startColumn + (stop - start) + 1;
                }
            }
            errors.add(new Error(startLine, stopLine, startColumn, stopColumn, msg, e));
        }
    }
}
