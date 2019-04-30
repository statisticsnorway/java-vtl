package no.ssb.vtl.script.expressions;

/*-
 * ========================LICENSE_START=================================
 * Java VTL
 * %%
 * Copyright (C) 2016 - 2018 Hadrien Kohl
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

import no.ssb.vtl.model.FilteringSpecification;
import no.ssb.vtl.model.VTLBoolean;
import no.ssb.vtl.model.VTLExpression;
import no.ssb.vtl.model.VTLObject;
import no.ssb.vtl.model.VtlFiltering;
import no.ssb.vtl.script.expressions.equality.AbstractEqualityExpression;
import no.ssb.vtl.script.expressions.equality.EqualExpression;
import no.ssb.vtl.script.expressions.equality.GraterThanExpression;
import no.ssb.vtl.script.expressions.equality.GreaterOrEqualExpression;
import no.ssb.vtl.script.expressions.equality.IsNotNullExpression;
import no.ssb.vtl.script.expressions.equality.IsNullExpression;
import no.ssb.vtl.script.expressions.equality.LesserOrEqualExpression;
import no.ssb.vtl.script.expressions.equality.LesserThanExpression;
import no.ssb.vtl.script.expressions.equality.NotEqualExpression;
import no.ssb.vtl.script.expressions.logic.AndExpression;
import no.ssb.vtl.script.expressions.logic.OrExpression;

import javax.script.SimpleBindings;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.lang.String.format;

public class VtlFilteringConverter {

    public static VtlFiltering convert(AbstractEqualityExpression equalityExpression) {
        VTLExpression leftOperand = equalityExpression.getLeftOperand();
        VTLExpression rightOperand = equalityExpression.getRightOperand();

        // We only support filter in the form of variable OP literal.
        if ((leftOperand instanceof VariableExpression && rightOperand instanceof LiteralExpression) ||
                (leftOperand instanceof LiteralExpression && rightOperand instanceof VariableExpression)) {
            VariableExpression variableExpression = (VariableExpression) (leftOperand instanceof VariableExpression ?
                    leftOperand : rightOperand);
            LiteralExpression literalExpression = (LiteralExpression) (leftOperand instanceof LiteralExpression ?
                    leftOperand : rightOperand);

            // Use the internal identifier with dataset prefix if it is a membership expression.
            // This should be refactored at some point.
            String column = "";
            if (variableExpression instanceof MembershipExpression) {
                column = ((MembershipExpression) variableExpression).getDatasetIdentifier() + "_";
            }
            column = column + variableExpression.getIdentifier();

            VTLObject value = literalExpression.resolve(null);
            if (equalityExpression instanceof EqualExpression) {
                return VtlFiltering.eq(column, value.get());
            } else if (equalityExpression instanceof NotEqualExpression) {
                return VtlFiltering.neq(column, value.get());
            } else if (equalityExpression instanceof GreaterOrEqualExpression) {
                return VtlFiltering.ge(column, value.get());
            } else if (equalityExpression instanceof GraterThanExpression) {
                return VtlFiltering.gt(column, value.get());
            } else if (equalityExpression instanceof LesserOrEqualExpression) {
                return VtlFiltering.le(column, value.get());
            } else if (equalityExpression instanceof LesserThanExpression) {
                return VtlFiltering.lt(column, value.get());
            } else if (equalityExpression instanceof IsNullExpression) {
                if (equalityExpression instanceof IsNotNullExpression) {
                    return VtlFiltering.neq(column, value.get());
                } else {
                    return VtlFiltering.eq(column, value.get());
                }
            }
        }
        // Unsupported expressions are converted to TRUE.
        return VtlFiltering.literal(false, FilteringSpecification.Operator.TRUE, null, null);
    }

    public static VtlFiltering convert(VTLExpression predicate) {
        if (!predicate.getVTLType().equals(VTLBoolean.class)) {
            throw new IllegalArgumentException(format("predicate %s was not a boolean", predicate));
        }

        if (predicate instanceof OrExpression) {
            return convert((OrExpression) predicate);
        } else if (predicate instanceof AndExpression) {
            return convert((AndExpression) predicate);
        } else if (predicate instanceof AbstractEqualityExpression) {
            return convert((AbstractEqualityExpression) predicate);
        } else if (predicate instanceof LiteralExpression) {
            VTLBoolean value = (VTLBoolean) predicate.resolve(new SimpleBindings(Collections.emptyMap()));
            return VtlFiltering.literal(!value.get(), FilteringSpecification.Operator.TRUE, null, value);
        }
        // TODO: Handle XorExpression.
        return VtlFiltering.literal(false, FilteringSpecification.Operator.TRUE, null, null);
    }

    public static VtlFiltering convert(OrExpression orExpression) {
        List<VtlFiltering> ops = new ArrayList<>();
        if (orExpression.getLeftOperand() instanceof OrExpression) {
            ops.addAll(convert((OrExpression) orExpression.getLeftOperand()).getOperands());
        } else {
            ops.add(convert(orExpression.getLeftOperand()));
        }
        VtlFiltering sub = convert(orExpression.getRightOperand());
        if (sub != null && sub.getOperator() != FilteringSpecification.Operator.TRUE) {
            ops.add(sub);
        }

        return VtlFiltering.or(ops.toArray(new VtlFiltering[0]));
    }

    public static VtlFiltering convert(AndExpression andExpression) {
        List<VtlFiltering> ops = new ArrayList<>();
        if (andExpression.getLeftOperand() instanceof AndExpression) {
            ops.addAll(convert((AndExpression) andExpression.getLeftOperand()).getOperands());
        } else {
            ops.add(convert(andExpression.getLeftOperand()));
        }
        ops.add(convert(andExpression.getRightOperand()));
        return VtlFiltering.and(ops.toArray(new VtlFiltering[0]));
    }
}
