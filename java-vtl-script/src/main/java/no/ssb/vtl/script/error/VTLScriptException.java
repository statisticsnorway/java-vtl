package no.ssb.vtl.script.error;

/*
 * ========================LICENSE_START=================================
 * Java VTL
 * %%
 * Copyright (C) 2016 - 2017 Hadrien Kohl
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *      http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * =========================LICENSE_END==================================
 */

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;

import javax.script.ScriptException;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Subclass of {@link ScriptException} that exposes start and end positions.
 */
public class VTLScriptException extends ScriptException implements PositionableError {

    private static final String UNKNOWN_SOURCE_NAME = "<unknown>";
    private String fileName = UNKNOWN_SOURCE_NAME;

    private int startLine = -1;
    private int startColumn = -1;

    private int stopLine = -1;
    private int stopColumn = -1;

    public VTLScriptException(Exception e, ParserRuleContext ctx) {
        super(e.getMessage());
        extractPositionFromContext(ctx);
    }

    public VTLScriptException(String message, ParserRuleContext ctx) {
        super(message);
        extractPositionFromContext(ctx);
    }

    public VTLScriptException(String msg, int line, int column) {
        super(msg);
        this.startLine = line;
        this.startColumn = column;
    }

    public VTLScriptException(String msg, int startLine, int startColumn, int stopLine, int stopColumn) {
        super(msg);

        this.startColumn = startColumn;
        this.startLine = startLine;

        this.stopColumn = stopColumn;
        this.stopLine = stopLine;
    }


    private void extractPositionFromContext(ParserRuleContext ctx) {
        checkNotNull(ctx);
        startLine = ctx.getStart().getLine();
        startColumn = ctx.getStart().getCharPositionInLine();
        if (ctx.getStop() != null) {
            stopLine = ctx.getStop().getLine();
            if (ctx.getStart() == ctx.getStop()) {
                Token token = ctx.getStart();
                stopColumn = startColumn + token.getText().length();
            } else {
                stopColumn = ctx.getStop().getCharPositionInLine();
            }
        }
    }

    @Override
    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = Optional.ofNullable(fileName).orElse(UNKNOWN_SOURCE_NAME);
    }

    @Override
    public String getMessage() {
        String message = super.getMessage();
        message += " in " + getFileName();
        message += " at line number " + getLineNumber();
        message += " at column number " + getColumnNumber();
        return message;
    }

    @Override
    public int getLineNumber() {
        return getStartLine();
    }

    @Override
    public int getColumnNumber() {
        return getStartColumn();
    }

    @Override
    public int getStartLine() {
        return startLine;
    }

    @Override
    public int getStopLine() {
        return stopLine;
    }

    @Override
    public int getStartColumn() {
        return startColumn;
    }

    @Override
    public int getStopColumn() {
        return stopColumn;
    }
}
