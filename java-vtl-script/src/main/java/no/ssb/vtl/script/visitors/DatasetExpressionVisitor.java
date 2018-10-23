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

import com.google.common.collect.Lists;
import no.ssb.vtl.model.Dataset;
import no.ssb.vtl.model.VTLExpression;
import no.ssb.vtl.parser.VTLBaseVisitor;
import no.ssb.vtl.parser.VTLParser;
import no.ssb.vtl.script.VTLDataset;
import no.ssb.vtl.script.error.ContextualRuntimeException;
import no.ssb.vtl.script.operations.union.UnionOperation;
import no.ssb.vtl.script.visitors.join.JoinBodyVisitor;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A visitor that handles the relational operators.
 */
public class DatasetExpressionVisitor extends VTLBaseVisitor<Dataset> {

    private final ExpressionVisitor expressionVisitor;

    public DatasetExpressionVisitor(ExpressionVisitor expressionVisitor) {
        this.expressionVisitor = checkNotNull(expressionVisitor);
        //this.joinVisitor = new JoinExpressionVisitor(context);
    }

    @Override
    public Dataset visitVariable(VTLParser.VariableContext ctx) {
        VTLExpression expression2 = expressionVisitor.visit(ctx);
        VTLDataset resolved = (VTLDataset) expression2.resolve(expressionVisitor.getBindings());
        return resolved.get();
    }

    @Override
    public Dataset visitUnionExpression(VTLParser.UnionExpressionContext ctx) {
        List<Dataset> datasets = Lists.newArrayList();
        for (VTLParser.DatasetExpressionContext datasetExpressionContext : ctx.datasetExpression()) {
            datasets.add(visit(datasetExpressionContext));
        }
        try {
            return new UnionOperation(datasets);
        } catch (Exception e) {
            throw new ContextualRuntimeException(e, ctx);
        }

    }

    @Override
    public Dataset visitJoinExpression(VTLParser.JoinExpressionContext ctx) {
        JoinBodyVisitor joinBodyVisitor = new JoinBodyVisitor(this);
        return joinBodyVisitor.visit(ctx);
    }
}
