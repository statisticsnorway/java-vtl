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

import com.google.common.collect.ImmutableMap;
import no.ssb.vtl.model.Component;
import no.ssb.vtl.model.Dataset;
import no.ssb.vtl.parser.VTLParser;
import no.ssb.vtl.script.error.ContextualRuntimeException;
import no.ssb.vtl.script.operations.join.AbstractJoinOperation;
import no.ssb.vtl.script.operations.join.CommonIdentifierBindings;
import no.ssb.vtl.script.operations.join.InnerJoinOperation;
import no.ssb.vtl.script.operations.join.OuterJoinOperation;
import no.ssb.vtl.script.visitors.ComponentVisitor;
import no.ssb.vtl.script.visitors.DatasetExpressionVisitor;
import no.ssb.vtl.script.visitors.VTLDatasetExpressionVisitor;
import org.antlr.v4.runtime.Token;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Visitor that handle the join definition.
 * <p>
 * TODO: Need to support alias here. The spec forgot it.
 * TODO: Support for escaped identifiers.
 */
public class JoinDefinitionVisitor extends VTLDatasetExpressionVisitor<AbstractJoinOperation> {

    private DatasetExpressionVisitor datasetExpressionVisitor;

    public JoinDefinitionVisitor(DatasetExpressionVisitor datasetExpressionVisitor) {
        this.datasetExpressionVisitor = checkNotNull(datasetExpressionVisitor);
    }

    private ImmutableMap<String, Dataset> extractDatasets(List<VTLParser.VariableContext> variables) {
        ImmutableMap.Builder<String, Dataset> builder = ImmutableMap.builder();
        for (VTLParser.VariableContext variable : variables) {
            // TODO: Escaped identifiers.
            String name = variable.getText();
            Dataset dataset = datasetExpressionVisitor.visit(variable);
            builder.put(name, dataset);
        }
        return builder.build();
    }

    private ImmutableMap<String, Component> extractIdentifierComponents(List<VTLParser.VariableContext> identifiers,
                                                                ImmutableMap<String, Dataset> datasets) {
        ImmutableMap.Builder<String, Component> builder = ImmutableMap.builder();
        ComponentVisitor componentVisitor = new ComponentVisitor(new CommonIdentifierBindings(datasets));
        for (VTLParser.VariableContext identifier : identifiers) {
            Component identifierComponent = componentVisitor.visit(identifier);
            builder.put(identifier.getText(), identifierComponent);
        }
        return builder.build();
    }

    @Override
    public AbstractJoinOperation visitJoinDefinition(VTLParser.JoinDefinitionContext ctx) {

        // Create a component bindings to be able to resolve components.
        ImmutableMap<String, Dataset> datasets = extractDatasets(ctx.datasets.variable());

        ImmutableMap<String, Component> identifiers = ctx.identifiers == null
                ? ImmutableMap.of()
                : extractIdentifierComponents(ctx.identifiers.variable(), datasets);

        Integer joinType = Optional.ofNullable(ctx.type).map(Token::getType).orElse(VTLParser.INNER);
        switch (joinType) {
            case VTLParser.INNER:
                return new InnerJoinOperation(datasets, identifiers);
            case VTLParser.OUTER:
                return new OuterJoinOperation(datasets, identifiers);
            case VTLParser.CROSS:
                //TODO: Finish CrossJoinOperation
                throw new ContextualRuntimeException("Not implemented", ctx);


        }
        return super.visitJoinDefinition(ctx);
    }
}
