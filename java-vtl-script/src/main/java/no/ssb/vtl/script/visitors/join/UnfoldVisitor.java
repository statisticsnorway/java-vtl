package no.ssb.vtl.script.visitors.join;

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


import com.google.common.collect.Sets;
import no.ssb.vtl.model.Component;
import no.ssb.vtl.model.Dataset;
import no.ssb.vtl.parser.VTLParser;
import no.ssb.vtl.script.operations.unfold.UnfoldOperation;
import no.ssb.vtl.script.visitors.ComponentVisitor;
import no.ssb.vtl.script.visitors.LiteralVisitor;
import no.ssb.vtl.script.visitors.VTLDatasetExpressionVisitor;

import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

public class UnfoldVisitor extends VTLDatasetExpressionVisitor<UnfoldOperation> {

    private final Dataset dataset;
    private final ComponentVisitor componentVisitor;
    private final LiteralVisitor literalVisitor = LiteralVisitor.getInstance();

    public UnfoldVisitor(Dataset dataset, ComponentVisitor componentVisitor) {
        this.dataset = checkNotNull(dataset);
        this.componentVisitor = componentVisitor;
    }

    @Override
    public UnfoldOperation visitJoinUnfoldExpression(VTLParser.JoinUnfoldExpressionContext ctx) {
        Component dimension = componentVisitor.visit(ctx.dimension);
        Component measure = componentVisitor.visit(ctx.measure);

        Set<String> elements = Sets.newLinkedHashSet();
        for (VTLParser.StringLiteralContext strLit : ctx.stringLiteral()) {
            elements.add(literalVisitor.visitStringLiteral(strLit).get());
        }
        return new UnfoldOperation(dataset, dimension, measure, elements);
    }
}
