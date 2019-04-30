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

import no.ssb.vtl.model.Dataset;
import no.ssb.vtl.model.VTLBoolean;
import no.ssb.vtl.model.VTLExpression;
import no.ssb.vtl.model.VTLNumber;
import no.ssb.vtl.model.VTLObject;
import no.ssb.vtl.model.VTLTyped;
import no.ssb.vtl.parser.VTLBaseVisitor;
import no.ssb.vtl.parser.VTLParser;
import no.ssb.vtl.script.VTLDataset;
import no.ssb.vtl.script.error.ContextualRuntimeException;
import no.ssb.vtl.script.error.VTLRuntimeException;
import no.ssb.vtl.script.expressions.FunctionExpression;
import no.ssb.vtl.script.expressions.IfThenElseExpression;
import no.ssb.vtl.script.expressions.LiteralExpression;
import no.ssb.vtl.script.expressions.MembershipExpression;
import no.ssb.vtl.script.expressions.VariableExpression;
import no.ssb.vtl.script.expressions.arithmetic.AdditionExpression;
import no.ssb.vtl.script.expressions.arithmetic.DivisionExpression;
import no.ssb.vtl.script.expressions.arithmetic.MultiplicationExpression;
import no.ssb.vtl.script.expressions.arithmetic.SubtractionExpression;
import no.ssb.vtl.script.expressions.equality.EqualExpression;
import no.ssb.vtl.script.expressions.equality.GraterThanExpression;
import no.ssb.vtl.script.expressions.equality.GreaterOrEqualExpression;
import no.ssb.vtl.script.expressions.equality.IsNotNullExpression;
import no.ssb.vtl.script.expressions.equality.IsNullExpression;
import no.ssb.vtl.script.expressions.equality.LesserOrEqualExpression;
import no.ssb.vtl.script.expressions.equality.LesserThanExpression;
import no.ssb.vtl.script.expressions.equality.NotEqualExpression;
import no.ssb.vtl.script.expressions.logic.AndExpression;
import no.ssb.vtl.script.expressions.logic.NotExpression;
import no.ssb.vtl.script.expressions.logic.OrExpression;
import no.ssb.vtl.script.expressions.logic.XorExpression;
import no.ssb.vtl.script.functions.VTLConcatenation;
import no.ssb.vtl.script.visitors.functions.NativeFunctionsVisitor;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.antlr.v4.runtime.tree.ParseTree;

import javax.script.Bindings;
import java.util.function.Predicate;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;

/**
 * TODO: extend abstract variable visitor.
 * TODO: Reduce complexity.
 */
public class ExpressionVisitor extends VTLBaseVisitor<VTLExpression> {

    private final LiteralVisitor literalVisitor = LiteralVisitor.getInstance();
    private final NativeFunctionsVisitor nativeFunctionsVisitor = new NativeFunctionsVisitor(this);
    private final Bindings scope;

    public ExpressionVisitor(Bindings scope) {
        this.scope = checkNotNull(scope);
    }

    private static boolean isNull(VTLTyped typed) {
        return VTLObject.class.equals(typed.getVTLType());
    }

    private static String checkVariableExist(Bindings bindings, VTLParser.VariableContext ctx) {
        String identifier = ctx.getText();
        // TODO: Remove escape logic.
        // TODO: Already in superclass
        identifier = unEscape(identifier);

        if (bindings.containsKey(identifier))
            return identifier;

        throw new ContextualRuntimeException(
                format("undefined variable %s", identifier),
                ctx
        );
    }

    private static String unEscape(String identifier) {
        // Unescape.
        if (identifier.startsWith("\'") && identifier.endsWith("\'")) {
            identifier = identifier.substring(1, identifier.length() - 1);
        }
        return identifier;
    }

    public Bindings getBindings() {
        return scope;
    }

    @Override
    public VTLExpression visitLiteral(VTLParser.LiteralContext ctx) {
        VTLObject literal = literalVisitor.visit(ctx);
        return new LiteralExpression(literal);
    }

    @Override
    public VTLExpression visitPostfixExpr(VTLParser.PostfixExprContext ctx) {
        VTLExpression operand = visit(ctx.expression());
        switch (ctx.op.getType()) {
            case VTLParser.ISNOTNULL:
                return new IsNotNullExpression(operand);
            case VTLParser.ISNULL:
                return new IsNullExpression(operand);
            default:
                throw new ParseCancellationException("unknown operator " + ctx.op.getText());
        }
    }

    @Override
    public VTLExpression visitPrecedenceExpr(VTLParser.PrecedenceExprContext ctx) {
        return visit(ctx.expression());
    }

    @Override
    public VTLExpression visitUnaryExpr(VTLParser.UnaryExprContext ctx) {
        VTLExpression operand = visitTyped(ctx.expression(), VTLBoolean.class);
        switch (ctx.op.getType()) {
            case VTLParser.NOT:
                return new NotExpression(operand);
            default:
                throw new ContextualRuntimeException("unknown operator " + ctx.op.getText(), ctx);
        }
    }

    @Override
    public VTLExpression visitArithmeticExpr(VTLParser.ArithmeticExprContext ctx) {

        // Check that the operands are of type number.
        VTLExpression leftExpression = visitTyped(ctx.left, VTLNumber.class);
        VTLExpression rightExpression = visitTyped(ctx.right, VTLNumber.class);

        switch (ctx.op.getType()) {
            case VTLParser.MUL:
                return new MultiplicationExpression(leftExpression, rightExpression);
            case VTLParser.DIV:
                return new DivisionExpression(leftExpression, rightExpression);
            case VTLParser.PLUS:
                return new AdditionExpression(leftExpression, rightExpression);
            case VTLParser.MINUS:
                return new SubtractionExpression(leftExpression, rightExpression);
            default:
                throw new ParseCancellationException("unknown operator " + ctx.op.getText());
        }
    }

    @Override
    public VTLExpression visitBinaryExpr(VTLParser.BinaryExprContext ctx) {
        switch (ctx.op.getType()) {

            case VTLParser.CONCAT:
                VTLExpression leftExpression = visit(ctx.left);
                VTLExpression rightExpression = visit(ctx.right);
                return new FunctionExpression<>(VTLConcatenation.getInstance(), leftExpression, rightExpression);

            case VTLParser.EQ:
            case VTLParser.NE:
            case VTLParser.LE:
            case VTLParser.LT:
            case VTLParser.GE:
            case VTLParser.GT:
                return getEqualityExpression(ctx);

            case VTLParser.AND:
            case VTLParser.OR:
            case VTLParser.XOR:
                return getBooleanExpression(ctx);

            default:
                throw new ParseCancellationException("unknown operator " + ctx.op.getText());
        }
    }

    private VTLExpression getEqualityExpression(VTLParser.BinaryExprContext ctx) {

        // TODO: Check that both expression can be compared.
        VTLExpression leftExpression = visit(ctx.left);
        VTLExpression rightExpression = visit(ctx.right);

        VTLExpression result;
        switch (ctx.op.getType()) {
            case VTLParser.EQ:
                result = new EqualExpression(leftExpression, rightExpression);
                break;
            case VTLParser.NE:
                result = new NotEqualExpression(leftExpression, rightExpression);
                break;
            case VTLParser.LE:
                result = new LesserOrEqualExpression(leftExpression, rightExpression);
                break;
            case VTLParser.LT:
                result = new LesserThanExpression(leftExpression, rightExpression);
                break;
            case VTLParser.GE:
                result = new GreaterOrEqualExpression(leftExpression, rightExpression);
                break;
            case VTLParser.GT:
                result = new GraterThanExpression(leftExpression, rightExpression);
                break;
            default:
                throw new ContextualRuntimeException("unknown equality operator " + ctx.op.getText(), ctx);
        }
        return result;
    }

    private VTLExpression getBooleanExpression(VTLParser.BinaryExprContext ctx) {

        // Check that the operands are of type boolean.
        VTLExpression leftExpression = visitTyped(ctx.left, VTLBoolean.class);
        VTLExpression rightExpression = visitTyped(ctx.right, VTLBoolean.class);

        VTLExpression result;
        switch (ctx.op.getType()) {
            case VTLParser.AND:
                result = new AndExpression(leftExpression, rightExpression);
                break;
            case VTLParser.OR:
                result = new OrExpression(leftExpression, rightExpression);
                break;
            case VTLParser.XOR:
                result = new XorExpression(leftExpression, rightExpression);
                break;
            default:
                throw new ContextualRuntimeException("unknown logic operator " + ctx.op.getText(), ctx);
        }
        return result;
    }

    /**
     * Same as {@link #visit(ParseTree)} but with type check.
     */
    public VTLExpression visitTyped(ParserRuleContext ctx, Class<? extends VTLObject> requiredType) {
        VTLExpression expression = visit(ctx);
        if (!isNull(expression) && !requiredType.isAssignableFrom(expression.getVTLType())) {
            throw new ContextualRuntimeException(
                    format(
                            "invalid type %s, expected %s",
                            expression.getVTLType().getSimpleName(),
                            requiredType.getSimpleName()
                    ),
                    ctx
            );
        }
        return expression;
    }

    @Override
    public VTLExpression visitNvlFunction(VTLParser.NvlFunctionContext ctx) {
        return nativeFunctionsVisitor.visit(ctx);
    }

    @Override
    public VTLExpression visitNativeFunctionCall(VTLParser.NativeFunctionCallContext ctx) {
        return nativeFunctionsVisitor.visit(ctx);
    }

    @Override
    public VTLExpression visitUserFunctionCall(VTLParser.UserFunctionCallContext ctx) {
        // TODO: look for function. Check signature.

        throw new ContextualRuntimeException(
                "undefined function " + ctx.functionName.getText(),
                ctx
        );
    }

    @Override
    public VTLExpression visitMembershipExpression(VTLParser.MembershipExpressionContext ctx) {
        String leftIdentifier = checkVariableExist(scope, ctx.left);
        Object object = scope.get(leftIdentifier);
        if (object instanceof Bindings) {
            Bindings bindings = (Bindings) object;
            String rightIdentifier = checkVariableExist(bindings, ctx.right);
            VTLTyped typed = (VTLTyped) bindings.get(rightIdentifier);
            return new MembershipExpression(typed.getVTLType(), leftIdentifier, rightIdentifier);
        } else {
            throw new UnsupportedOperationException("[" + leftIdentifier + "] was not a dataset");
        }
    }

    @Override
    public VTLExpression visitVariable(VTLParser.VariableContext ctx) {
        String identifier = checkVariableExist(scope, ctx);
        Object object = scope.get(identifier);
        if (object instanceof VTLTyped) {
            Class type = ((VTLTyped) object).getVTLType();
            return new VariableExpression(type, identifier);
        }
        if (object instanceof Dataset) {
            return new VTLExpression() {
                @Override
                public VTLObject resolve(Bindings bindings) {
                    return VTLDataset.of((Dataset) bindings.get(identifier));
                }

                @Override
                public Class getVTLType() {
                    return VTLDataset.class;
                }
            };
        }
        throw new VTLRuntimeException(
                format("unknown object [%s]", object), "VTL-101", ctx
        );
    }

    @Override
    public VTLExpression visitIfThenElseExpression(VTLParser.IfThenElseExpressionContext ctx) {
        IfThenElseExpression.Builder builder = new IfThenElseExpression.Builder(visit(ctx.ifBodyExpression()));

        for (VTLParser.IfBodyContext bodyContext : ctx.ifBody()) {
            try {
                VTLExpression condition = visit(bodyContext.ifBodyExpression(0));
                VTLExpression value = visit(bodyContext.ifBodyExpression(1));
                builder.addCondition(condition, value);
            } catch (IllegalArgumentException iae) {
                throw new ContextualRuntimeException(iae, bodyContext);
            }
        }

        try {
            return builder.build();
        } catch (IllegalArgumentException iae) {
            throw new ContextualRuntimeException(iae, ctx);
        }
    }
}
