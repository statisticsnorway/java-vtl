package kohl.hadrien.vtl.script.error;

import static kohl.hadrien.vtl.script.error.VTLErrorCodeUtil.checkVTLCode;

/**
 * Thrown when the VTL received a syntactically invalid expression.
 */
public class SyntaxException extends CompilationException {

    private static final long serialVersionUID = 5524299192944792558L;
    private final String VTLCode;

    public SyntaxException(String s, String vtlCode) {
        super(s);
        this.VTLCode = checkVTLCode(vtlCode, "VTL-01");
    }

    public SyntaxException(Exception e, String vtlCode) {
        super(e);
        this.VTLCode = checkVTLCode(vtlCode, "VTL-01");
    }

    public SyntaxException(String message, String fileName, int lineNumber, String vtlCode) {
        super(message, fileName, lineNumber);
        this.VTLCode = checkVTLCode(vtlCode, "VTL-01");
    }

    public SyntaxException(String message, String fileName, int lineNumber, int columnNumber, String vtlCode) {
        super(message, fileName, lineNumber, columnNumber);
        this.VTLCode = checkVTLCode(vtlCode, "VTL-01");
    }

    @Override
    public String getVTLCode() {
        return this.VTLCode;
    }
}
