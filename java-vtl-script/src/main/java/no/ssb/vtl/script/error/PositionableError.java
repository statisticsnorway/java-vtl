package kohl.hadrien.vtl.script.error;

/**
 * Mark the exception as positionable.
 */
public interface PositionableError {

    void setLine(int line);

    void setColumn(int column);

    int getLineNumber();

    int getColumnNumber();
}
