package no.ssb.vtl.script.visitors;

/*-
 * ========================LICENSE_START=================================
 * Java VTL
 * %%
 * Copyright (C) 2016 - 2017 Hadrien Kohl
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
 * =========================LICENSE_END==================================
 */

import com.google.common.annotations.VisibleForTesting;
import no.ssb.vtl.model.VTLBoolean;
import no.ssb.vtl.model.VTLDate;
import no.ssb.vtl.model.VTLFloat;
import no.ssb.vtl.model.VTLInteger;
import no.ssb.vtl.model.VTLObject;
import no.ssb.vtl.model.VTLString;
import no.ssb.vtl.parser.VTLBaseVisitor;
import no.ssb.vtl.parser.VTLParser;

import java.time.Instant;

public final class LiteralVisitor extends VTLBaseVisitor<VTLObject> {

    private static final LiteralVisitor instance = new LiteralVisitor();
    private static final String QUOTE_CHAR = "(?<!\")\"(?!\")";
    private static final String ESCAPED_QUOTE_CHAR = "\"\"";

    private LiteralVisitor() {
        // private
    }

    public static LiteralVisitor getInstance() {
        return instance;
    }

    @VisibleForTesting
    static String stripQuotes(String string) {
        return string.replaceAll(QUOTE_CHAR, "").replaceAll(ESCAPED_QUOTE_CHAR, "\"");
    }

    @Override
    public VTLString visitStringLiteral(VTLParser.StringLiteralContext ctx) {
        return VTLString.of(stripQuotes(ctx.getText()));
    }

    @Override
    public VTLBoolean visitBooleanLiteral(VTLParser.BooleanLiteralContext ctx) {
        return VTLBoolean.of(Boolean.valueOf(ctx.getText()));
    }

    @Override
    public VTLInteger visitIntegerLiteral(VTLParser.IntegerLiteralContext ctx) {
        return VTLInteger.of(Long.valueOf(ctx.getText()));
    }

    @Override
    public VTLDate visitDateLiteral(VTLParser.DateLiteralContext ctx) {
        return VTLDate.of(Instant.parse(ctx.getText()));
    }

    @Override
    public VTLFloat visitFloatLiteral(VTLParser.FloatLiteralContext ctx) {
        return VTLFloat.of(Double.valueOf(ctx.getText()));
    }

    @Override
    public VTLObject visitNullLiteral(VTLParser.NullLiteralContext ctx) {
        return VTLObject.of((Object) null);
    }
}
