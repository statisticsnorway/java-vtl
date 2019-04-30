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
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import no.ssb.vtl.model.Component;
import no.ssb.vtl.model.Dataset;
import no.ssb.vtl.parser.VTLParser.AggregateAvgContext;
import no.ssb.vtl.parser.VTLParser.AggregateSumContext;
import no.ssb.vtl.parser.VTLParser.AggregationParamsContext;
import no.ssb.vtl.parser.VTLParser.VariableContext;
import no.ssb.vtl.parser.VTLParser.VariableExpressionContext;
import no.ssb.vtl.script.error.ContextualRuntimeException;
import no.ssb.vtl.script.functions.AggregationAvgFunction;
import no.ssb.vtl.script.functions.AggregationSumFunction;
import no.ssb.vtl.script.operations.aggregation.AggregationOperation;
import no.ssb.vtl.script.operations.aggregation.AbstractAggregationFunction;
import no.ssb.vtl.script.operations.join.ComponentBindings;
import org.antlr.v4.runtime.Token;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;
import static java.util.Optional.ofNullable;
import static no.ssb.vtl.parser.VTLParser.ALONG;
import static no.ssb.vtl.parser.VTLParser.GROUP_BY;

public class AggregationVisitor extends VTLDatasetExpressionVisitor<AggregationOperation> {

    private static final String NOT_SAME_DATASET_ERROR = "variable %s does not belong to dataset %s";
    private static final String NOT_AN_IDENTIFIER_ERROR = "variable %s was not an identifier";

    private final DatasetExpressionVisitor datasetExpressionVisitor;

    public AggregationVisitor(DatasetExpressionVisitor datasetExpressionVisitor) {
        this.datasetExpressionVisitor = checkNotNull(datasetExpressionVisitor);
    }

    private static Set<Component> computeMeasureComponentList(VariableExpressionContext datasetContext, Dataset dataset, ComponentVisitor componentVisitor) {
        return ofNullable(datasetContext.membershipExpression())
                .map(membershipExpressionContext -> membershipExpressionContext.right)
                .map(variableContext -> Collections.singleton(componentVisitor.visit(variableContext)))
                .orElse(dataset.getDataStructure().values().stream().filter(Component::isMeasure).collect(Collectors.toSet()));
    }

    private static Set<Component> computeAggregationComponents(Set<Component> aggregationComponents, Set<Component> availableIdentifiers, Token clause) {
        switch (clause.getType()) {
            case GROUP_BY:
                return aggregationComponents;
            case ALONG:
                return availableIdentifiers.stream()
                        .filter(component -> !aggregationComponents.contains(component))
                        .collect(Collectors.toSet());
            default:
                throw new IllegalArgumentException("unrecognized token: " + clause.getText());
        }
    }

    private static VariableContext extractComponentContext(VariableExpressionContext variableExpressionContext) {
        return ofNullable(variableExpressionContext.membershipExpression())
                .map(membershipContext -> membershipContext.right)
                .orElse(variableExpressionContext.variable());
    }

    private static VariableContext extractDatasetContext(VariableExpressionContext variableExpressionContext) {
        return ofNullable(variableExpressionContext.membershipExpression())
                .map(membershipContext -> membershipContext.left)
                .orElse(variableExpressionContext.variable());
    }

    @VisibleForTesting
    static AggregationOperation getAggregationOperation(
            Dataset dataset,
            List<Component> groupBy,
            List<Component> aggregationComponents,
            AbstractAggregationFunction aggregationFunction) {
        return new AggregationOperation(dataset, groupBy, aggregationComponents, aggregationFunction);
    }

    private static void checkComponentType(VariableExpressionContext parameterVariableContext, VariableContext variableContext, Component identifier) {
        if (!identifier.isIdentifier()) {
            throw new ContextualRuntimeException(
                    format(NOT_AN_IDENTIFIER_ERROR, variableContext.getText()),
                    parameterVariableContext
            );
        }
    }

    /**
     * Checks that the dataset in the variable expression uses the same dataset.
     */
    private static void checkDatasetContexts(VariableContext datasetContext, VariableExpressionContext parameterVariableContext) {
        Optional<VariableContext> datasetVariableCtx = ofNullable(parameterVariableContext.membershipExpression())
                .map(membershipContext -> membershipContext.left);

        if (!datasetVariableCtx.isPresent())
            return;

        if (!datasetVariableCtx.get().getText().equals(datasetContext.getText())) {
            throw new ContextualRuntimeException(
                    format(NOT_SAME_DATASET_ERROR, parameterVariableContext.getText(), datasetContext.getText()),
                    parameterVariableContext
            );
        }
    }

    @Override
    public AggregationOperation visitAggregateSum(AggregateSumContext ctx) {
        return getAggregationOperation(
                ctx.variableExpression(),
                ctx.aggregationParams(),
                new AggregationSumFunction()
        );
    }

    @Override
    public AggregationOperation visitAggregateAvg(AggregateAvgContext ctx) {
        return getAggregationOperation(
                ctx.variableExpression(),
                ctx.aggregationParams(),
                new AggregationAvgFunction()
        );
    }


    private AggregationOperation getAggregationOperation(
            VariableExpressionContext variableExpressionContext,
            AggregationParamsContext paramContexts,
            AbstractAggregationFunction aggregationFunction) {

        // Get the context that represents our dataset variable.
        VariableContext datasetContext = extractDatasetContext(variableExpressionContext);

        // Create a component visitor for the dataset.
        Dataset dataset = datasetExpressionVisitor.visit(datasetContext);
        ComponentVisitor componentVisitor = new ComponentVisitor(new ComponentBindings(dataset));

        // All measures or only the one selected.
        Set<Component> measureComponents = computeMeasureComponentList(
                variableExpressionContext,
                dataset,
                componentVisitor
        );

        Set<Component> aggregationComponents = Sets.newHashSet();

        for (VariableExpressionContext parameterVariableContext : paramContexts.variableExpression()) {

            // Check that the dataset names are the same.
            checkDatasetContexts(datasetContext, parameterVariableContext);

            VariableContext variableContext = extractComponentContext(parameterVariableContext);
            Component identifier = componentVisitor.visit(variableContext);

            checkComponentType(parameterVariableContext, variableContext, identifier);

            aggregationComponents.add(identifier);
        }

        Set<Component> availableIdentifiers = dataset.getDataStructure().values().stream()
                .filter(Component::isIdentifier)
                .collect(Collectors.toSet());

        Set<Component> components = computeAggregationComponents(aggregationComponents, availableIdentifiers, paramContexts.aggregationClause);

        return getAggregationOperation(dataset, Lists.newArrayList(components), Lists.newArrayList(measureComponents), aggregationFunction);
    }
}
