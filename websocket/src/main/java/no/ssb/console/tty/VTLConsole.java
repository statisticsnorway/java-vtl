package kohl.hadrien.console.tty;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import io.termd.core.readline.Function;
import io.termd.core.readline.Keymap;
import io.termd.core.readline.Readline;
import io.termd.core.tty.TtyConnection;
import io.termd.core.util.Helper;
import kohl.hadrien.console.tty.parsing.ParsingResult;
import kohl.hadrien.console.tty.parsing.SyntaxError;
import kohl.hadrien.console.tty.parsing.SyntaxErrorListener;
import kohl.hadrien.vtl.model.Component;
import no.ssb.vtl.model.DataStructure;
import no.ssb.vtl.model.Dataset;
import kohl.hadrien.vtl.parser.VTLLexer;
import kohl.hadrien.vtl.parser.VTLParser;
import kohl.hadrien.vtl.script.VTLScriptEngine;
import no.ssb.vtl.connectors.SsbApiConnector;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.tree.ParseTree;

import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static java.lang.String.format;

/**
 * A termd interpreter.
 */
public class VTLConsole implements Consumer<TtyConnection> {

    private static final String MOTD = "" +
            "\nJava VTL interpreter version "
            + VTLConsole.class.getPackage().getImplementationVersion() + ".\n" +
            "Type .help for command list.\n\n";

    private static final String VTL = "" +
            "    ___       ___       ___   \n" +
            "   /\\__\\     /\\  \\     /\\__\\  \n" +
            "  /:/ _/_    \\:\\  \\   /:/  /  \n" +
            " |::L/\\__\\   /::\\__\\ /:/__/   \n" +
            " |::::/  /  /:/\\/__/ \\:\\  \\   \n" +
            "  L;;/__/   \\/__/     \\:\\__\\  \n" +
            "                       \\/__/\n";

    private ScriptEngine engine;

    @Override
    public void accept(TtyConnection ttyConnection) {
        InputStream inputrc = Keymap.class.getResourceAsStream("inputrc");
        Keymap keymap = new Keymap(inputrc);
        Readline readline = new Readline(keymap);

        List<Function> functions = Helper.loadServices(Thread.currentThread().getContextClassLoader(), Function.class);
        for (io.termd.core.readline.Function function : functions) {
            readline.addFunction(function);
        }

        readline.addFunction(new Function() {
            @Override
            public String name() {
                return "test";
            }

            @Override
            public void apply(Readline.Interaction interaction) {
                System.err.print(interaction);
            }
        });

        ttyConnection.write(VTL);
        ttyConnection.write(MOTD);

        // TODO: Use engine.getContext().setAttribute() to setup the connector.
        // ScriptEngineManager manager = new ScriptEngineManager();
        // engine = checkNotNull(manager.getEngineByName("VTLJava"));

        engine = new VTLScriptEngine(new SsbApiConnector(new ObjectMapper()));


        read(ttyConnection, readline);
    }

    private void read(TtyConnection ttyConnection, Readline readline) {
        readline.readline(ttyConnection, "vtl> ", line -> {
            boolean readAgain = true;
            try {
                if (!line.isEmpty()) {
                    if (line.trim().startsWith(".")) {
                        try {
                            readAgain = evalCommand(ttyConnection, line.trim());
                        } catch (Exception e) {
                            ttyConnection.write(e.getMessage());
                        }
                    } else {
                        ParsingResult parsingResult = parseVTL(line);
                        if (parsingResult.syntaxErrorListener.getSyntaxErrors().isEmpty())
                            engine.eval(line);

                        printErrors(ttyConnection, parsingResult);
                    }
                }
            } catch (ScriptException e) {
                StringWriter stack = new StringWriter();
                e.printStackTrace(new PrintWriter(stack));
                ttyConnection.write(stack.toString());
            } finally {
                if (readAgain)
                    read(ttyConnection, readline);
                else
                    ttyConnection.close();
            }
        });
    }

    private Boolean evalCommand(TtyConnection ttyConnection, String command) throws IOException {
        if (".help".equals(command)) {
            ttyConnection.write("" +
                    "start typing a VTL expression or one " +
                    "of the folowing commands:\n" +
                    "\t .help\tshow this message\n" +
                    "\t .exit\texit the console\n" +
                    "\t .show\tdisplay the content of a variable\n" +
                    "\t .list\tlist the declared variable\n" +
                    "\t .connectors\tget information about the available" +
                    " connectors\n");
            return true;
        }
        if (".exit".equals(command) || ".quit".equals(command)) {
            ttyConnection.write("Exiting...\n");
            return false;
        }
        if (".exit".equals(command)) {
            ttyConnection.write("Exiting...\n");
            return false;
        }
        if (command.startsWith(".show")) {
            Bindings bindings = engine.getBindings(ScriptContext.ENGINE_SCOPE);
            // TODO: Use regexp.
            if (!bindings.containsKey(command.split(" ")[1])) {
                ttyConnection.write("variable not found\n");
            } else {
                printDataset(ttyConnection, (Dataset) bindings.get(command.split(" ")[1]));
            }
            return true;
        }
        if (".list".equals(command)) {
            Bindings bindings = engine.getBindings(ScriptContext.ENGINE_SCOPE);
            for (String name : bindings.keySet()) {
                ttyConnection.write(name + "\t->\t" + bindings.get(name) + "\n");
            }
            return true;
        }
        ttyConnection.write("Unrecognized command\n");
        return true;
    }

    private void printDataset(TtyConnection ttyConnection, Dataset dataset) throws IOException {
        DataStructure dataStructure = dataset.getDataStructure();

        // Header
        List<String> columns = Lists.newArrayList();
        for (Component component : dataStructure.values()) {
            columns.add(
                    format(
                            "%s[%s,%s]",
                            component.getName(),
                            component.getType().getSimpleName(),
                            component.getType().getSimpleName()
                    )
            );
        }
        ttyConnection.write(columns.stream().collect(Collectors.joining(",")) + "\n");

        // Rows
        Iterator<Dataset.Tuple> iterator = dataset.stream().iterator();
        while (iterator.hasNext()) {
            columns.clear();
            Dataset.Tuple row = iterator.next();
            columns = row.stream().map(dataPoint -> {
                return dataPoint.get() == null ? "[NULL]" : dataPoint.get().toString();
            }).collect(Collectors.toList());
            ttyConnection.write(columns.stream().collect(Collectors.joining(",")) + "\n");
        }
    }

    private void printErrors(TtyConnection connection, ParsingResult parsingResult) {
        for (SyntaxError syntaxError : parsingResult.syntaxErrorListener.getSyntaxErrors()) {
            int a, b; // Start and stop index
            RecognitionException cause = syntaxError.getException();
            if (cause instanceof LexerNoViableAltException) {
                a = ((LexerNoViableAltException) cause).getStartIndex();
                b = ((LexerNoViableAltException) cause).getStartIndex() + 1;
            } else {
                Token offendingToken = (Token) syntaxError.getOffendingSymbol();
                a = offendingToken.getStartIndex();
                b = offendingToken.getStopIndex() + 1;
            }
            // TODO: Highlight errors.
            String errorString = String.format("\033[31msyntax error:(%d,%d-%s) %s.\033[39;49m\n", 1, a, b, syntaxError.getMessage());
            connection.write(errorString);
        }
    }

    ParsingResult parseVTL(String text) {
        ANTLRInputStream input = new ANTLRInputStream(text);
        VTLLexer lexer = new VTLLexer(input);
        VTLParser parser = new VTLParser(new CommonTokenStream(lexer));

        SyntaxErrorListener listener = new SyntaxErrorListener();
        parser.removeErrorListeners();
        parser.addErrorListener(listener);
        lexer.removeErrorListeners();
        lexer.addErrorListener(listener);

        ParseTree t = parser.start();
        return new ParsingResult(parser, t, listener);
    }
}
