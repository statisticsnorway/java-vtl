package kohl.hadrien.vtl.script.error;

import static kohl.hadrien.vtl.script.error.VTLErrorCodeUtil.checkVTLCode;

/**
 * Thrown when the VTL received an expression with type error.
 */
public class TypeException extends CompilationException {
    private static final long serialVersionUID = 9220460392615795969L;
    private final String VTLCode;

    public TypeException(String s, String vtlCode) {
        super(s);
        this.VTLCode = checkVTLCode(vtlCode, "VTL-02");
    }

    public TypeException(Exception e, String vtlCode) {
        super(e);
        this.VTLCode = checkVTLCode(vtlCode, "VTL-02");
    }

    public TypeException(String message, String fileName, int lineNumber, String vtlCode) {
        super(message, fileName, lineNumber);
        this.VTLCode = checkVTLCode(vtlCode, "VTL-02");
    }

    public TypeException(String message, String fileName, int lineNumber, int columnNumber, String vtlCode) {
        super(message, fileName, lineNumber, columnNumber);
        this.VTLCode = checkVTLCode(vtlCode, "VTL-02");
    }

    @Override
    public String getVTLCode() {
        return VTLCode;
    }
}
