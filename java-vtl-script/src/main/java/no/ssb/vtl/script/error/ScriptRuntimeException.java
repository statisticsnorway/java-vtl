package kohl.hadrien.vtl.script.error;

import javax.script.ScriptException;

import static kohl.hadrien.vtl.script.error.VTLErrorCodeUtil.checkVTLCode;

/**
 * Thrown when the VTL execution failed at runtime.
 */
public class ScriptRuntimeException extends ScriptException implements VTLThrowable {

    private static final long serialVersionUID = 7253953869019949964L;
    private final String VTLCode;

    public ScriptRuntimeException(String s, String vtlCode) {
        super(s);
        this.VTLCode = checkVTLCode(vtlCode, "VTL-1");
    }

    public ScriptRuntimeException(Exception e, String vtlCode) {
        super(e);
        this.VTLCode = checkVTLCode(vtlCode, "VTL-1");
    }

    public ScriptRuntimeException(String message, String fileName, int lineNumber, String vtlCode) {
        super(message, fileName, lineNumber);
        this.VTLCode = checkVTLCode(vtlCode, "VTL-1");
    }

    public ScriptRuntimeException(String message, String fileName, int lineNumber, int columnNumber, String vtlCode) {
        super(message, fileName, lineNumber, columnNumber);
        this.VTLCode = checkVTLCode(vtlCode, "VTL-1");
    }

    @Override
    public String getVTLCode() {
        return VTLCode;
    }
}
