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

import com.google.common.collect.ImmutableSet;
import no.ssb.vtl.model.Dataset;
import no.ssb.vtl.parser.VTLParser;
import no.ssb.vtl.script.operations.fold.FoldOperation;
import no.ssb.vtl.script.visitors.ComponentVisitor;
import no.ssb.vtl.script.visitors.VTLDatasetExpressionVisitor;

import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;
import static no.ssb.vtl.script.visitors.AbstractVariableVisitor.unEscape;

public class FoldVisitor extends VTLDatasetExpressionVisitor<FoldOperation> {

    private final Dataset dataset;
    private final ComponentVisitor componentVisitor;

    public FoldVisitor(Dataset dataset, ComponentVisitor componentVisitor) {
        this.dataset = checkNotNull(dataset);
        this.componentVisitor = checkNotNull(componentVisitor);
    }

    @Override
    public FoldOperation visitJoinFoldExpression(VTLParser.JoinFoldExpressionContext ctx) {
        String dimension = unEscape(ctx.dimension.getText());
        String measure = unEscape(ctx.measure.getText());

        Set<String> elements = ctx.variableExpression().stream()
                .map(componentVisitor::visit)
                .map(dataset.getDataStructure()::getName)
                .collect(ImmutableSet.toImmutableSet());

        return new FoldOperation(dataset, dimension, measure, elements);
    }
}
