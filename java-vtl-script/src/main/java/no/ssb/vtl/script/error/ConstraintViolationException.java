package kohl.hadrien.vtl.script.error;

import javax.script.ScriptException;

import static kohl.hadrien.vtl.script.error.VTLErrorCodeUtil.checkVTLCode;

/**
 * Thrown when the VTL received does not respect operators preconditions.
 */
public class ConstraintViolationException extends ScriptException implements VTLThrowable {

    private static final long serialVersionUID = -617398492563000321L;

    private final String VTLCode;

    public ConstraintViolationException(String s, String vtlCode) {
        super(s);
        this.VTLCode = checkVTLCode(vtlCode, "VTL-03");
    }

    public ConstraintViolationException(Exception e, String vtlCode) {
        super(e);
        this.VTLCode = checkVTLCode(vtlCode, "VTL-03");
    }

    public ConstraintViolationException(String message, String fileName, int lineNumber, String vtlCode) {
        super(message, fileName, lineNumber);
        this.VTLCode = checkVTLCode(vtlCode, "VTL-03");
    }

    public ConstraintViolationException(String message, String fileName, int lineNumber, int columnNumber, String vtlCode) {
        super(message, fileName, lineNumber, columnNumber);
        this.VTLCode = checkVTLCode(vtlCode, "VTL-03");
    }

    @Override
    public String getVTLCode() {
        return VTLCode;
    }
}
