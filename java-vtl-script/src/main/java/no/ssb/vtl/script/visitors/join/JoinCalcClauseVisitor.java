package no.ssb.vtl.script.join;

import no.ssb.vtl.model.DataPoint;
import no.ssb.vtl.model.Dataset;
import no.ssb.vtl.parser.VTLBaseVisitor;
import no.ssb.vtl.parser.VTLParser;

import java.util.function.Function;

import static java.lang.String.format;

/**
 * Visitor for join calc clauses.
 */
public class JoinCalcClauseVisitor extends VTLBaseVisitor<Function<Dataset.Tuple, Object>> {

    @Override
    public Function<Dataset.Tuple, Object> visitJoinCalcReference(VTLParser.JoinCalcReferenceContext ctx) {
        String variableName = ctx.getText();
        return new Function<Dataset.Tuple, Object>() {
            @Override
            public Object apply(Dataset.Tuple tuple) {
                for (DataPoint dataPoint : tuple) {
                    if (variableName.equals(dataPoint.getName())) {
                        return dataPoint.get();
                    }
                }
                throw new RuntimeException(format("variable %s not found", variableName));
            }
        };
    }

    @Override
    public Function<Dataset.Tuple, Object> visitJoinCalcRef(VTLParser.JoinCalcRefContext ctx) {
        return super.visitJoinCalcRef(ctx);
    }


    @Override
    public Function<Dataset.Tuple, Object> visitJoinCalcAtom(VTLParser.JoinCalcAtomContext ctx) {
        VTLParser.ConstantContext constantValue = ctx.constant();
        if (constantValue.FLOAT_CONSTANT() != null)
            return tuple -> {
                return Float.valueOf(constantValue.FLOAT_CONSTANT().getText());
            };
        if (constantValue.INTEGER_CONSTANT() != null)
            return tuple -> {
                return Integer.valueOf(constantValue.INTEGER_CONSTANT().getText());
            };

        throw new RuntimeException(
                format("unsuported constant type %s", constantValue)
        );
    }

    @Override
    public Function<Dataset.Tuple, Object> visitJoinCalcPrecedence(VTLParser.JoinCalcPrecedenceContext ctx) {
        return visit(ctx.joinCalcExpression());
    }

    @Override
    public Function<Dataset.Tuple, Object> visitJoinCalcSummation(VTLParser.JoinCalcSummationContext ctx) {
        Function<Dataset.Tuple, Object> leftResult = visit(ctx.leftOperand);
        Function<Dataset.Tuple, Object> rightResult = visit(ctx.rightOperand);

        // Check types?
        //checkArgument(Number.class.isAssignableFrom(leftResult.getType()));
        //checkArgument(Number.class.isAssignableFrom(rightResult.getType()));

        return tuple -> {

            Number leftNumber = (Number) leftResult.apply(tuple);
            Number rightNumber = (Number) rightResult.apply(tuple);

            // TODO: Write test
            if (leftNumber == null || rightNumber == null) {
                return null;
            }

            // TODO: document boxing and overflow
            if (leftNumber instanceof Float || rightNumber instanceof Float) {
                if (ctx.sign.getText().equals("+")) {
                    return leftNumber.floatValue() + rightNumber.floatValue();
                } else {
                    return leftNumber.floatValue() - rightNumber.floatValue();
                }
            }
            if (leftNumber instanceof Double || rightNumber instanceof Double) {
                if (ctx.sign.getText().equals("+")) {
                    return leftNumber.doubleValue() + rightNumber.doubleValue();
                } else {
                    return leftNumber.doubleValue() - rightNumber.doubleValue();
                }
            }
            if (leftNumber instanceof Integer || rightNumber instanceof Integer) {
                if (ctx.sign.getText().equals("+")) {
                    return leftNumber.intValue() + rightNumber.intValue();
                } else {
                    return leftNumber.intValue() - rightNumber.intValue();
                }
            }
            if (leftNumber instanceof Long || rightNumber instanceof Long) {
                if (ctx.sign.getText().equals("+")) {
                    return leftNumber.longValue() + rightNumber.longValue();
                } else {
                    return leftNumber.longValue() - rightNumber.longValue();
                }
            }

            throw new RuntimeException(
                    format("unsupported number types %s, %s", leftNumber.getClass(), rightNumber.getClass())
            );
        };
    }


    @Override
    public Function<Dataset.Tuple, Object> visitJoinCalcProduct(VTLParser.JoinCalcProductContext ctx) {
        Function<Dataset.Tuple, Object> leftResult = visit(ctx.leftOperand);
        Function<Dataset.Tuple, Object> rightResult = visit(ctx.rightOperand);

        // Check types?
        //checkArgument(Number.class.isAssignableFrom(leftResult.getType()));
        //checkArgument(Number.class.isAssignableFrom(rightResult.getType()));

        return tuple -> {
            Number leftNumber = (Number) leftResult.apply(tuple);
            Number rightNumber = (Number) rightResult.apply(tuple);

            if (leftNumber == null ^ rightNumber == null) {
                return null;
            }

            // TODO: document boxing and overflow
            if (leftNumber instanceof Float || rightNumber instanceof Float) {
                if (ctx.sign.getText().equals("*")) {
                    return leftNumber.floatValue() * rightNumber.floatValue();
                } else {
                    return leftNumber.floatValue() / rightNumber.floatValue();
                }
            }
            if (leftNumber instanceof Double || rightNumber instanceof Double) {
                if (ctx.sign.getText().equals("*")) {
                    return leftNumber.doubleValue() * rightNumber.doubleValue();
                } else {
                    return leftNumber.doubleValue() / rightNumber.doubleValue();
                }
            }
            if (leftNumber instanceof Integer || rightNumber instanceof Integer) {
                if (ctx.sign.getText().equals("*")) {
                    return leftNumber.intValue() * rightNumber.intValue();
                } else {
                    return leftNumber.intValue() / rightNumber.intValue();
                }
            }
            if (leftNumber instanceof Long || rightNumber instanceof Long) {
                if (ctx.sign.getText().equals("*")) {
                    return leftNumber.longValue() * rightNumber.longValue();
                } else {
                    return leftNumber.longValue() / rightNumber.longValue();
                }
            }

            throw new RuntimeException(
                    format("unsupported number types %s, %s", leftNumber.getClass(), rightNumber.getClass())
            );

        };
    }

}
