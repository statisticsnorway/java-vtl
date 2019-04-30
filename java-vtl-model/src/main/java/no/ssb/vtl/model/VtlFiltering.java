package no.ssb.vtl.model;

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

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BinaryOperator;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;

/**
 * Representation of a filter
 */
public abstract class VtlFiltering implements Filtering {

    protected final String column;
    protected final VTLObject value;
    protected final Operator operator;
    protected final Set<VtlFiltering> operands;
    private final boolean negated;
    protected ToIntFunction<String> hashFunction;

    protected VtlFiltering(
            boolean negated,
            Operator operator,
            Collection<VtlFiltering> operands
    ) {
        if (!(operator == Operator.AND || operator == Operator.OR)) {
            throw new IllegalArgumentException("This constructor support only OR and AND");
        }
        this.negated = negated;
        this.operator = operator;
        this.value = null;
        this.column = null;
        this.operands = new LinkedHashSet<>(operands);
    }

    protected VtlFiltering(
            boolean negated,
            String column,
            Operator operator,
            VTLObject value
    ) {
        if (operator == Operator.AND || operator == Operator.OR) {
            throw new IllegalArgumentException("This constructor support only EQ, LT, GT");
        }
        this.negated = negated;
        this.operator = operator;
        this.column = column;
        this.value = value;
        this.operands = Collections.emptySet();
    }

    public static Builder using(DataStructure structure) {
        return new Builder(structure);
    }

    public static Builder using(Dataset dataset) {
        return new Builder(dataset.getDataStructure());
    }

    public static VtlFiltering not(VtlFiltering filtering) {
        if (filtering.getOperator() == Operator.AND || filtering.getOperator() == Operator.OR) {
            return nary(!filtering.isNegated(), filtering.getOperator(), filtering.getOperands());
        } else {
            return literal(!filtering.isNegated(), filtering.getOperator(), filtering.getColumn(), filtering.getValue());
        }
    }

    public static VtlFiltering eq(String column, Object value) {
        return new Literal(false, column, VTLObject.of(value), Operator.EQ);
    }

    public static VtlFiltering neq(String column, Object value) {
        return new Literal(true, column, VTLObject.of(value), Operator.EQ);
    }

    public static VtlFiltering gt(String column, Object value) {
        return new Literal(false, column, VTLObject.of(value), Operator.GT);
    }

    public static VtlFiltering lt(String column, Object value) {
        return new Literal(false, column, VTLObject.of(value), Operator.LT);
    }

    public static VtlFiltering ge(String column, Object value) {
        return new Literal(true, column, VTLObject.of(value), Operator.LT);
    }

    public static VtlFiltering le(String column, Object value) {
        return new Literal(true, column, VTLObject.of(value), Operator.GT);
    }

    public static VtlFiltering and(VtlFiltering... operands) {
        return new And(false, operands);
    }

    public static VtlFiltering or(VtlFiltering... operands) {
        return new Or(false, operands);
    }

    public static VtlFiltering or(Collection<VtlFiltering> operands) {
        return new Or(false, operands);
    }

    public static VtlFiltering nary(boolean negated, Operator operator, Collection<VtlFiltering> operands) {
        switch (operator) {
            case AND:
                return new And(negated, operands);
            case OR:
                return new Or(negated, operands);
            case EQ:
            case GT:
            case LT:
            case TRUE:
            default:
                throw new IllegalArgumentException("unsupported operator: " + operator);
        }
    }

    public static VtlFiltering nary(boolean negated, Operator operator, VtlFiltering... operands) {
        return nary(negated, operator, Arrays.asList(operands));
    }

    public static VtlFiltering literal(boolean negated, Operator operator, String column, VTLObject value) {
        switch (operator) {
            case EQ:
            case GT:
            case LT:
            case TRUE:
                return new Literal(negated, column, value, operator);
            case AND:
            case OR:
            default:
                throw new IllegalArgumentException("unsupported operator: " + operator);
        }
    }

    public static VtlFiltering literal(boolean negated, Operator operator, String column, Object value) {
        return literal(negated, operator, column, VTLObject.of(value));
    }

    public static VtlFiltering transform(VtlFiltering orders, BinaryOperator<VtlFiltering> transform) {
        if (orders.getOperator() == Operator.AND || orders.getOperator() == Operator.OR) {
            List<VtlFiltering> ops = new ArrayList<>();
            for (VtlFiltering operand : orders.getOperands()) {
                VtlFiltering op = transform(operand, transform);
                ops.add(op);
            }
            return nary(orders.isNegated(), orders.getOperator(), ops);
        } else {
            return transform.apply(null, orders);
        }
    }

    private void setHashFunction(ToIntFunction<String> function) {
        this.hashFunction = function;
        for (FilteringSpecification operand : getOperands()) {
            if (operand instanceof VtlFiltering) {
                ((VtlFiltering) operand).setHashFunction(hashFunction);
            }
        }
    }

    @Override
    public Collection<VtlFiltering> getOperands() {
        return this.operands;
    }

    @Override
    public Operator getOperator() {
        return this.operator;
    }

    @Override
    public String getColumn() {
        return this.column;
    }

    @Override
    public VTLObject getValue() {
        return this.value;
    }

    @Override
    public Boolean isNegated() {
        return this.negated;
    }

    public static class Builder {

        private final ToIntFunction<String> hashFunction;
        private VtlFiltering filter;

        public Builder(DataStructure structure) {
            ImmutableList<String> columns = ImmutableList.copyOf(structure.keySet());
            hashFunction = columns::indexOf;
        }

        public Builder and(VtlFiltering... operands) {
            if (filter == null) {
                filter = new And(false, operands);
            }
            return this;
        }

        public Builder or(VtlFiltering... operands) {
            if (filter == null) {
                filter = new Or(false, operands);
            }
            return this;
        }

        public VtlFiltering with(VtlFiltering filter) {
            if (this.filter != null) {
                throw new IllegalArgumentException("filter already created");
            }
            this.filter = filter;
            return build();
        }

        /**
         * Neutralize the literals of the given filter that cannot be send to the child.
         * <p>
         * If the filter is a literal, transform to TRUE if parent is AND or FALSE if parent is AND.
         */
        public VtlFiltering transposeInternal(FilteringSpecification original) {

            Operator operator = original.getOperator();
            Boolean negated = original.isNegated();

            if (original.getOperands().isEmpty()) {
                if (hashFunction.applyAsInt(original.getColumn()) >= 0) {
                    return new Literal(negated, original.getColumn(), original.getValue(), operator);
                } else {
                    return new Literal(false, original.getColumn(), original.getValue(), Operator.TRUE);
                }
            } else {
                List<VtlFiltering> ops = new ArrayList<>();
                for (FilteringSpecification operand : original.getOperands()) {
                    VtlFiltering transposed = transposeInternal(operand);
                    if (transposed != null) {
                        ops.add(transposed);
                    }
                }
                Optional<Boolean> constant = ops.stream()
                        .filter(filter -> filter.getOperator() == Operator.TRUE)
                        .findAny().map(vtlFiltering -> !vtlFiltering.isNegated());
                if (operator == Operator.AND) {
                    if (constant.isPresent() && !constant.get()) {
                        return new Literal(negated, null, null, Operator.TRUE);
                    } else {
                        return new And(negated, ops);
                    }
                } else {
                    if (constant.isPresent() && constant.get()) {
                        return new Literal(negated, null, null, Operator.TRUE);
                    } else {
                        return new Or(negated, ops);
                    }
                }
            }
        }

        /**
         * Neutralize the literals of the given filter that cannot be send to the child.
         */
        public VtlFiltering transpose(FilteringSpecification original) {
            VtlFiltering filtering = transposeInternal(original);
            filtering.setHashFunction(hashFunction);
            return filtering;
        }

        public VtlFiltering build() {
            filter.setHashFunction(hashFunction);
            return filter;
        }

    }

    static public class Literal extends VtlFiltering {

        Literal(boolean negated, String column, VTLObject value, Operator operator) {
            super(negated, column, operator, value);
        }

        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Filtering)) return false;

            Filtering that = (Filtering) o;

            if (!that.getOperands().isEmpty()) {
                return that.equals(this);
            }

            if (!Objects.equals(this.getOperator(), that.getOperator())) return false;
            if (!Objects.equals(this.isNegated(), that.isNegated())) return false;
            if (!Objects.equals(this.getColumn(), that.getColumn())) return false;
            if (!Objects.equals(this.getValue(), that.getValue())) return false;

            return true;
        }

        public int hashCode() {
            return Objects.hash(
                    this.getOperator(),
                    this.isNegated(),
                    this.getColumn(),
                    this.getValue()
            );
        }

        @Override
        public Operator getOperator() {
            return this.operator;
        }

        @Override
        public String getColumn() {
            return column;
        }

        @Override
        public VTLObject getValue() {
            return value;
        }

        @Override
        public boolean test(DataPoint dataPoint) {
            Operator operator = getOperator();
            if (operator == Operator.TRUE) {
                return !isNegated();
            }
            VTLObject columnValue = dataPoint.get(hashFunction.applyAsInt(getColumn()));
            int compare = columnValue.compareTo(value);
            switch (operator) {
                case EQ:
                    return compare == 0 ^ isNegated();
                case GT:
                    return compare > 0 ^ isNegated();
                case LT:
                    return compare < 0 ^ isNegated();
            }
            throw new IllegalArgumentException();
        }

        @Override
        public String toString() {
            switch (getOperator()) {
                case EQ:
                    return String.format("%s%s%s", column, isNegated() ? "!=" : "=", getValue());
                case GT:
                    return String.format("%s%s%s", column, isNegated() ? "<=" : ">", getValue());
                case LT:
                    return String.format("%s%s%s", column, isNegated() ? ">=" : "<", getValue());
                case TRUE:
                    return isNegated() ? "FALSE" : "TRUE";
                default:
                    return "unknown";
            }
        }
    }

    static public class And extends VtlFiltering {

        And(boolean negated, VtlFiltering... operands) {
            super(negated, Operator.AND, Arrays.asList(operands));
        }

        And(boolean negated, Collection<VtlFiltering> operands) {
            super(negated, Operator.AND, operands);
        }

        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Filtering)) return false;

            Filtering that = (Filtering) o;

            // FALSE & X => FALSE.
            Optional<VtlFiltering> containsFalse = this.getOperands().stream()
                    .filter(op -> op.isNegated() && op.getOperator().equals(Operator.TRUE))
                    .findFirst();
            if (containsFalse.isPresent()) {
                return that.equals(containsFalse.get());
            }

            // TRUE & X => X
            HashSet<VtlFiltering> thisOpWithoutTrue = this.getOperands().stream()
                    .filter(op -> !(!op.isNegated() && op.getOperator().equals(Operator.TRUE)))
                    .collect(Collectors.toCollection(HashSet::new));

            HashSet<Filtering> thatOpWithoutTrue = that.getOperands().stream()
                    .filter(op -> !(!op.isNegated() && op.getOperator().equals(Operator.TRUE)))
                    .collect(Collectors.toCollection(HashSet::new));

            if (!Objects.equals(thisOpWithoutTrue, thatOpWithoutTrue)) return false;

            if (!Objects.equals(this.getOperator(), that.getOperator())) return false;
            if (!Objects.equals(this.isNegated(), that.isNegated())) return false;

            return true;
        }

        public int hashCode() {
            return Objects.hash(
                    this.getOperator(),
                    this.isNegated(),
                    this.getColumn(),
                    this.getValue()
            );
        }

        @Override
        public boolean test(DataPoint dataPoint) {
            if (isNegated()) {
                // De Morgan's law
                for (VtlFiltering operand : operands) {
                    if (!operand.test(dataPoint)) {
                        return true;
                    }
                }
                return false;
            } else {
                for (VtlFiltering operand : operands) {
                    if (!operand.test(dataPoint)) {
                        return false;
                    }
                }
                return true;
            }
        }

        @Override
        public String toString() {
            StringBuilder stringBuilder = new StringBuilder();
            if (isNegated()) {
                stringBuilder.append("~");
            }
            stringBuilder.append(
                    operands.stream().map(Objects::toString)
                            .collect(Collectors.joining("&", "(", ")"))
            );
            return stringBuilder.toString();
        }

        @Override
        public Operator getOperator() {
            return Operator.AND;
        }

        @Override
        public String getColumn() {
            return null;
        }

        @Override
        public VTLObject getValue() {
            return null;
        }
    }

    static public class Or extends VtlFiltering {

        Or(boolean negated, VtlFiltering... operands) {
            super(negated, Operator.OR, Arrays.asList(operands));
        }

        Or(boolean negated, Collection<VtlFiltering> operands) {
            super(negated, Operator.OR, operands);
        }

        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Filtering)) return false;

            Filtering that = (Filtering) o;

            if (!Objects.equals(this.getOperator(), that.getOperator())) return false;
            if (!Objects.equals(this.isNegated(), that.isNegated())) return false;

            // TRUE | X => TRUE
            Optional<VtlFiltering> containsTrue = this.getOperands().stream()
                    .filter(op -> op.isNegated() && op.getOperator().equals(Operator.TRUE))
                    .findFirst();
            if (containsTrue.isPresent()) {
                return that.equals(containsTrue.get());
            }

            // FALSE | X => X.
            HashSet<VtlFiltering> thisOpWithoutTrue = this.getOperands().stream()
                    .filter(op -> op.isNegated() && op.getOperator().equals(Operator.TRUE))
                    .collect(Collectors.toCollection(HashSet::new));

            HashSet<VtlFiltering> thatOpWithoutTrue = this.getOperands().stream()
                    .filter(op -> op.isNegated() && op.getOperator().equals(Operator.TRUE))
                    .collect(Collectors.toCollection(HashSet::new));

            if (!Objects.equals(thisOpWithoutTrue, thatOpWithoutTrue)) return false;

            return true;
        }

        public int hashCode() {
            return Objects.hash(
                    this.getOperator(),
                    this.isNegated(),
                    this.getColumn(),
                    this.getValue()
            );
        }

        @Override
        public boolean test(DataPoint dataPoint) {
            if (isNegated()) {
                // De Morgan's law
                for (VtlFiltering operand : operands) {
                    if (!operand.test(dataPoint)) {
                        return false;
                    }
                }
                return true;
            } else {
                for (VtlFiltering operand : operands) {
                    if (operand.test(dataPoint)) {
                        return true;
                    }
                }
                return false;
            }
        }

        @Override
        public String toString() {
            StringBuilder stringBuilder = new StringBuilder();
            if (isNegated()) {
                stringBuilder.append("~");
            }
            stringBuilder.append(
                    operands.stream().map(Objects::toString)
                            .collect(Collectors.joining("|", "(", ")"))
            );
            return stringBuilder.toString();
        }

        @Override
        public String getColumn() {
            return null;
        }

        @Override
        public VTLObject getValue() {
            return null;
        }
    }
}
