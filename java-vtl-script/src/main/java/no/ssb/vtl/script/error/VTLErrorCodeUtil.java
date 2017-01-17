package kohl.hadrien.vtl.script.error;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Utility class for error codes.
 */
public class VTLErrorCodeUtil {

    public static String checkVTLCode(String vtlCode, String prefix) {
        checkNotNull(vtlCode, "vtl error code was null");
        checkArgument(
                vtlCode.startsWith(prefix),
                "vtl error code was expected to start with [%s], got [%s]", prefix, vtlCode);
        return vtlCode;
    }
}
