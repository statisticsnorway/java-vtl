package no.ssb.vtl.script.operations.visitors;

/*-
 * #%L
 * java-vtl-script
 * %%
 * Copyright (C) 2016 Hadrien Kohl
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
 * #L%
 */

import no.ssb.vtl.model.Dataset;
import no.ssb.vtl.parser.VTLBaseVisitor;
import no.ssb.vtl.parser.VTLParser;
import no.ssb.vtl.connector.Connector;

import javax.script.ScriptContext;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Assignement visitor.
 */
public class AssignmentVisitor extends VTLBaseVisitor<Dataset> {

    private final ScriptContext context;
    private final ConnectorVisitor connectorVisitor;
    private final ClauseVisitor clausesVisitor;
    private final RelationalVisitor relationalVisitor;

    public AssignmentVisitor(ScriptContext context, List<Connector> connectors) {
        this.context = checkNotNull(context, "the context was null");
        connectorVisitor = new ConnectorVisitor(connectors);
        clausesVisitor = new ClauseVisitor();
        relationalVisitor = new RelationalVisitor(this, context);
    }

    @Override
    protected Dataset aggregateResult(Dataset aggregate, Dataset nextResult) {
        return nextResult != null ? nextResult : aggregate;
    }

    @Override
    public Dataset visitStatement(VTLParser.StatementContext ctx) {
        String name = ctx.variableRef().getText();
        Dataset dataset = visit(ctx.datasetExpression());
        context.setAttribute(name, dataset, ScriptContext.ENGINE_SCOPE);
        return (Dataset) context.getAttribute(name, ScriptContext.ENGINE_SCOPE);
    }

    @Override
    public Dataset visitRelationalExpression(VTLParser.RelationalExpressionContext ctx) {
        Supplier<Dataset> datasetSupplier = relationalVisitor.visit(ctx);
        return datasetSupplier.get();
    }

    @Override
    public Dataset visitVariableRef(VTLParser.VariableRefContext ctx) {
        return (Dataset) context.getAttribute(ctx.getText());
    }

    @Override
    public Dataset visitGetExpression(VTLParser.GetExpressionContext ctx) {
        return connectorVisitor.visit(ctx);
    }

    @Override
    public Dataset visitPutExpression(VTLParser.PutExpressionContext ctx) {
        return connectorVisitor.visit(ctx);
    }

    @Override
    public Dataset visitWithClause(VTLParser.WithClauseContext ctx) {
        Dataset dataset = visit(ctx.datasetExpression());
        Function<Dataset, Dataset> clause = clausesVisitor.visit(ctx.clauseExpression());
        return clause.apply(dataset);
    }

}
