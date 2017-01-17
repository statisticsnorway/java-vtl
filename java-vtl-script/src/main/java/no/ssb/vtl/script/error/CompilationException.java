package kohl.hadrien.vtl.script.error;

import javax.script.ScriptException;

/**
 * Exception thrown when a compile time error arises.
 */
public abstract class CompilationException extends ScriptException implements VTLThrowable {
    private static final long serialVersionUID = 7934499985601545692L;

    public CompilationException(String s) {
        super(s);
    }

    public CompilationException(Exception e) {
        super(e);
    }

    public CompilationException(String message, String fileName, int lineNumber) {
        super(message, fileName, lineNumber);
    }

    public CompilationException(String message, String fileName, int lineNumber, int columnNumber) {
        super(message, fileName, lineNumber, columnNumber);
    }
}
