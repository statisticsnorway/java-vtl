package kohl.hadrien.vtl.script.error;

import javax.script.ScriptException;

/**
 * The validation exception is thrown when a validation fails in the
 * VTL script.
 */
public class ValidationException extends ScriptException {
    public ValidationException(String s) {
        super(s);
    }

    public ValidationException(Exception e) {
        super(e);
    }

    public ValidationException(String message, String fileName, int lineNumber) {
        super(message, fileName, lineNumber);
    }

    public ValidationException(String message, String fileName, int lineNumber, int columnNumber) {
        super(message, fileName, lineNumber, columnNumber);
    }
}
