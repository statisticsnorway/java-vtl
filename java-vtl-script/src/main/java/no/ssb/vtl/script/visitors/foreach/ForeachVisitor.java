package no.ssb.vtl.script.visitors.foreach;

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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import no.ssb.vtl.model.Dataset;
import no.ssb.vtl.model.VTLExpression;
import no.ssb.vtl.parser.VTLBaseVisitor;
import no.ssb.vtl.script.VTLDataset;
import no.ssb.vtl.script.operations.foreach.ForeachOperation;
import no.ssb.vtl.script.visitors.AssignmentVisitor;
import no.ssb.vtl.script.visitors.ExpressionVisitor;

import static com.google.common.base.Preconditions.checkNotNull;
import static no.ssb.vtl.parser.VTLParser.RepeatContext;
import static no.ssb.vtl.parser.VTLParser.StatementContext;
import static no.ssb.vtl.parser.VTLParser.VariableContext;

public class ForeachVisitor extends VTLBaseVisitor<ForeachOperation> {

    private final ExpressionVisitor expressionVisitor;

    public ForeachVisitor(ExpressionVisitor expressionVisitor) {
        this.expressionVisitor = checkNotNull(expressionVisitor);
    }

    @Override
    public ForeachOperation visitRepeat(RepeatContext ctx) {

        ImmutableMap.Builder<String, Dataset> datasets = ImmutableMap.builder();
        for (VariableContext variableCtx : ctx.datasets.variable()) {
            VTLExpression variable = expressionVisitor.visitTyped(
                    variableCtx, VTLDataset.class
            );
            datasets.put(
                    variableCtx.getText(),
                    (Dataset) variable.resolve(expressionVisitor.getBindings()).get()
            );
        }

        // TODO: Handle if only one dataset.
        ImmutableSet.Builder<String> identifiers = ImmutableSet.builder();
        for (VariableContext identifier : ctx.identifiers.variable()) {
            // TODO: handle unescape.
            identifiers.add(identifier.getText());
        }

        ForeachOperation foreachOperation = new ForeachOperation(datasets.build(), identifiers.build());

        foreachOperation.setBlock(bindings -> {
            AssignmentVisitor assignmentVisitor = new AssignmentVisitor(bindings);
            Object last = null;
            for (StatementContext statementContext : ctx.statement()) {
                last = assignmentVisitor.visit(statementContext);
            }
            return VTLDataset.of((Dataset) last);
        });

        return foreachOperation;
    }
}
