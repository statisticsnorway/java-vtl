package kohl.hadrien.vtl.interpreter;

/*-
 * #%L
 * java-vtl-script
 * %%
 * Copyright (C) 2016 Hadrien Kohl
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import jline.TerminalFactory;
import jline.console.ConsoleReader;
import kohl.hadrien.vtl.model.DataPoint;
import kohl.hadrien.vtl.model.Dataset;
import kohl.hadrien.vtl.script.VTLScriptEngine;
import no.ssb.vtl.connectors.SsbApiConnector;
import org.fusesource.jansi.AnsiConsole;

import javax.script.ScriptException;
import java.io.*;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static kohl.hadrien.vtl.model.Component.Role;

/**
 * A VTL interpreter.
 */
public class Interpreter implements Runnable {

    private static final String MOTD = "" +
            "Java VTL interpreter version " + Interpreter.class.getPackage().getImplementationVersion() + ".\n" +
            "Type .help for command list.";

    private final VTLValidator validator;
    private final ConsoleReader console;
    private final VTLScriptEngine vtlScriptEngine;

    public Interpreter(String... args) throws IOException {
        console = setupConsole();
        vtlScriptEngine = setupEngine();
        validator = new VTLValidator();

    }

    public static void main(String... args) throws IOException {

        Interpreter interpreter = new Interpreter();
        Thread thread = new Thread(interpreter);

        try {
            thread.start();
            thread.join();
        } catch (Throwable t) {
            thread.interrupt();
            t.printStackTrace(System.err);
        }

    }

    private static void printDataset(PrintStream output, Dataset dataset) {
        // Quickly print stream for now.
        for (String name : dataset.getDataStructure().keySet()) {
            output.print(name);
            output.print("[");
            output.print(dataset.getDataStructure().getRoles().get(name).name());
            output.print(",");
            output.print(dataset.getDataStructure().getTypes().get(name).getSimpleName());
            output.print("]");
        }
        output.println();
        for (Dataset.Tuple tuple : (Iterable<Dataset.Tuple>) dataset.stream()::iterator) {
            for (DataPoint component : tuple) {
                output.print(component.get());
                output.print(",");
            }
            output.println();
        }
    }



    private VTLScriptEngine setupEngine() {
        return new VTLScriptEngine(new SsbApiConnector(new ObjectMapper()));
    }

    private ConsoleReader setupConsole() throws IOException {
        ConsoleReader console;
        console = new ConsoleReader(
                "Java VTL",
                new FileInputStream(FileDescriptor.in),
                new FileOutputStream(FileDescriptor.out),
                TerminalFactory.create()
        );

        console.setPrompt("vtl> ");

        return console;
    }

    @Override
    public void run() {
        try {
            console.println(MOTD);

            repl();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                console.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void repl() throws IOException {

        PrintWriter output = new PrintWriter(console.getOutput());
        PrintStream error = System.err;
        String read;
        while ((read = console.readLine()) != null) {

            if ("".equals(read))
                continue;

            // Commands.
            if (read.trim().startsWith(".")) {
                if (!executeCommand(read.trim()))
                    break;
                continue;
            }


            if (!validator.isValidStatement(read)) {
                AnsiConsole.systemInstall();
                console.getOutput().write(validator.getError());
                AnsiConsole.systemUninstall();
                continue;
            }

            try {
                Object result = vtlScriptEngine.eval(read);

                if (result instanceof Dataset)
                    printDataset((Dataset) result);

                output.println(result);
                output.flush();
            } catch (ScriptException se) {
                error.println(se.getMessage());
            }
        }
        console.flush();
    }

    private boolean executeCommand(String command) throws IOException {
        if (command.startsWith(".help"))
            return printHelp();

        if (command.startsWith(".exit") || command.startsWith(".quit"))
            return false;

        console.println("Unknown command" + command);
        return true;

    }

    private boolean printHelp() throws IOException {
        console.println(".quit");
        console.println(".exit");
        console.println(".show datasetExpression");
        return true;
    }

    private void printDataset(Dataset dataset) throws IOException {
        Map<String, Role> roles = dataset.getDataStructure().getRoles();
        Map<String, Class<?>> types = dataset.getDataStructure().getTypes();

        // Header
        List<String> columns = Lists.newArrayList();
        for (String name : dataset.getDataStructure().keySet())
            columns.add(format("%s[%s,%s]", name, roles.get(name).name(), types.get(name).getSimpleName()));
        console.println(columns.stream().collect(Collectors.joining(",")));

        // Rows
        Iterator<Dataset.Tuple> iterator = dataset.stream().iterator();
        while (iterator.hasNext()) {
            columns.clear();
            Dataset.Tuple row = iterator.next();
            //Map<String, Object> asMap = row.stream().collect(Collectors.toMap(
            //        DataPoint::getName, DataPoint::get
            //));
            //for (String name : dataset.getDataStructure().keySet())
            //    columns.add(asMap.get(name).toString());

            console.println(row.stream()
                    .map(dataPoint -> {
                        return dataPoint.get() != null ? dataPoint.get().toString() : "[NULL]";
                    })
                    .collect(Collectors.joining(",")));
        }
    }

}
