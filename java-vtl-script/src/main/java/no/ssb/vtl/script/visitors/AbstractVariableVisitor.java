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

import no.ssb.vtl.parser.VTLBaseVisitor;
import no.ssb.vtl.parser.VTLParser;
import no.ssb.vtl.script.error.ContextualRuntimeException;

import javax.script.Bindings;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;

/**
 * Abstract visitor that fails if variables are not found.
 */
public abstract class AbstractVariableVisitor<T> extends VTLBaseVisitor<T> {

    private final Bindings bindings;

    public AbstractVariableVisitor(Bindings bindings) {
        this.bindings = checkNotNull(bindings);
    }

    private static String checkVariableExist(Bindings bindings, VTLParser.VariableContext ctx) {
        String identifier = ctx.getText();
        identifier = unEscape(identifier);

        if (bindings.containsKey(identifier))
            return identifier;

        throw new ContextualRuntimeException(format("undefined variable %s", ctx.getText()), ctx);
    }

    public static String unEscape(String identifier) {
        // Unescape.
        if (identifier.startsWith("\'") && identifier.endsWith("\'")) {
            return identifier.substring(1, identifier.length() - 1);
        }
        return identifier;
    }

    protected abstract T convert(Object o);

    @Override
    public T visitMembershipExpression(VTLParser.MembershipExpressionContext ctx) {
        String leftIdentifier = checkVariableExist(bindings, ctx.left);
        Object object = bindings.get(leftIdentifier);
        if (object instanceof Bindings) {
            Bindings bindings = (Bindings) object;
            String rightIdentifier = checkVariableExist(bindings, ctx.right);
            return convert(bindings.get(rightIdentifier));
        } else {
            return convert(object);
        }
    }

    @Override
    public T visitVariable(VTLParser.VariableContext ctx) {
        String identifier = checkVariableExist(bindings, ctx);
        return convert(bindings.get(identifier));
    }
}
