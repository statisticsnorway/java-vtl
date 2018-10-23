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

import no.ssb.vtl.model.Component;
import no.ssb.vtl.model.Dataset;
import no.ssb.vtl.model.VTLExpression;
import no.ssb.vtl.parser.VTLBaseVisitor;
import no.ssb.vtl.parser.VTLParser;
import no.ssb.vtl.script.operations.JoinAssignment;
import no.ssb.vtl.script.operations.join.AbstractJoinOperation;
import no.ssb.vtl.script.operations.join.ComponentBindings;
import no.ssb.vtl.script.visitors.ComponentRoleVisitor;
import no.ssb.vtl.script.visitors.ComponentVisitor;
import no.ssb.vtl.script.visitors.DatasetExpressionVisitor;
import no.ssb.vtl.script.visitors.ExpressionVisitor;

import java.util.Optional;

import static com.google.common.base.MoreObjects.firstNonNull;
import static java.util.Optional.ofNullable;
import static no.ssb.vtl.script.visitors.AbstractVariableVisitor.unEscape;

public class JoinBodyVisitor extends VTLBaseVisitor<Dataset> {

    private final JoinDefinitionVisitor joinDefVisitor;

    private ExpressionVisitor expressionVisitor;
    private Dataset workingDataset;
    private ComponentBindings componentBindings;
    private static final ComponentRoleVisitor ROLE_VISITOR = ComponentRoleVisitor.getInstance();
    private ComponentVisitor componentVisitor;

    public JoinBodyVisitor(DatasetExpressionVisitor datasetExpressionVisitor) {
        joinDefVisitor = new JoinDefinitionVisitor(datasetExpressionVisitor);
    }

    @Override
    public Dataset visitJoinExpression(VTLParser.JoinExpressionContext ctx) {
        // Get the abstract join operation (inner, outer, cross).

        AbstractJoinOperation joinOperation = joinDefVisitor.visit(ctx.joinDefinition());

        // Holds the component references.
        componentBindings = joinOperation.getJoinScope();
        workingDataset = joinOperation;
        expressionVisitor = new ExpressionVisitor(componentBindings);
        componentVisitor = new ComponentVisitor(componentBindings);

        return visit(ctx.joinBody());
    }
    
    @Override
    protected Dataset aggregateResult(Dataset aggregate, Dataset nextResult) {
        Dataset currentDataset = firstNonNull(nextResult, aggregate);
        // Update the component scope.
        // We might need to make bindings immutable and link the scope with each other.
        componentBindings.putAll(new ComponentBindings(currentDataset));
        return workingDataset = currentDataset;
    }

    @Override
    public Dataset visitJoinCalcClause(VTLParser.JoinCalcClauseContext ctx) {
        // The expression visitor operates on a scope that contains the component reference.
        VTLParser.JoinAssignmentContext joinAssignment = ctx.joinAssignment();

        Optional<Component.Role> componentRole = ofNullable(ROLE_VISITOR.visitComponentRole(joinAssignment.role));
        Boolean implicit = joinAssignment.implicit != null;

        VTLExpression expression = expressionVisitor.visit(joinAssignment.expression());

        // Calculate name
        String componentName = unEscape(joinAssignment.variable().getText());


        JoinAssignment result = new JoinAssignment(
                workingDataset,
                expression,
                componentName,
                componentRole.orElse(Component.Role.MEASURE),
                implicit,
                componentBindings
        );
        componentBindings.putAll(new ComponentBindings(result));
        return result;
    }

    @Override
    public Dataset visitJoinFoldClause(VTLParser.JoinFoldClauseContext ctx) {
        FoldVisitor visitor = new FoldVisitor(workingDataset, componentVisitor);
        return visitor.visit(ctx);
    }

    @Override
    public Dataset visitJoinUnfoldClause(VTLParser.JoinUnfoldClauseContext ctx) {
        UnfoldVisitor visitor = new UnfoldVisitor(workingDataset, componentVisitor);
        return visitor.visit(ctx);
    }

    @Override
    public Dataset visitJoinKeepClause(VTLParser.JoinKeepClauseContext ctx) {
        KeepVisitor visitor = new KeepVisitor(workingDataset, componentVisitor);
        return visitor.visit(ctx);
    }

    @Override
    public Dataset visitJoinDropClause(VTLParser.JoinDropClauseContext ctx) {
        DropVisitor visitor = new DropVisitor(workingDataset, componentVisitor);
        return visitor.visit(ctx);
    }

    @Override
    public Dataset visitJoinFilterClause(VTLParser.JoinFilterClauseContext ctx) {
        FilterVisitor visitor = new FilterVisitor(workingDataset, componentBindings, expressionVisitor);
        return visitor.visit(ctx);
    }

    @Override
    public Dataset visitJoinRenameClause(VTLParser.JoinRenameClauseContext ctx) {
        RenameVisitor visitor = new RenameVisitor(workingDataset, componentVisitor);
        return visitor.visit(ctx);
    }

}
