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

import no.ssb.vtl.model.Component;
import no.ssb.vtl.model.DataStructure;
import no.ssb.vtl.model.VTLExpression;
import no.ssb.vtl.model.VTLObject;
import no.ssb.vtl.parser.VTLBaseVisitor;
import no.ssb.vtl.parser.VTLParser;
import no.ssb.vtl.script.error.TypeException;
import org.antlr.v4.runtime.misc.ParseCancellationException;

import java.util.Map;
import java.util.Optional;

import static java.lang.String.*;

public class ConditionalExpressionVisitor extends VTLBaseVisitor<VTLExpression> {

    private final ReferenceVisitor referenceVisitor;
    private final DataStructure dataStructure;

    public ConditionalExpressionVisitor(ReferenceVisitor referenceVisitor, DataStructure dataStructure) {
        this.referenceVisitor = referenceVisitor;
        this.dataStructure = dataStructure;
    }

    @Override
    public VTLExpression visitNvlExpression(VTLParser.NvlExpressionContext ctx) {
        ParamVisitor paramVisitor = new ParamVisitor(referenceVisitor);
        Component input = (Component) paramVisitor.visit(ctx.componentRef());
        Object repValue = paramVisitor.visit(ctx.nvlRepValue);

        //TODO should work more with VTLObject. Now we mix own type with Java type.
        if (!input.getType().isAssignableFrom(repValue.getClass())) {
            throw new ParseCancellationException(new TypeException("The value to replace null must be of type " + input.getType()
            + ", but was: " + repValue.getClass() + ". Replacement value was: " + repValue, "VTL-02xx"));
        }

        return new VTLExpression.Builder(input.getType(), dataPoint -> {
            Map<Component, VTLObject> map = dataStructure.asMap(dataPoint);
            Optional<VTLObject> vtlObject = Optional.ofNullable(map.get(input));
            if (!vtlObject.isPresent()) {
                throw new RuntimeException(
                        format("Component %s not found in data structure", input));
            }
            if (vtlObject.get().get() == null) {
                return VTLObject.of(repValue);
            } else {
                return vtlObject.get();
            }

        }).description(format("nvl(%s, %s)", input, repValue)).build();
    }
}
