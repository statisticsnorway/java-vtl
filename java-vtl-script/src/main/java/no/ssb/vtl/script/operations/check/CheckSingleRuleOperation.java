package no.ssb.vtl.script.operations.check;

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

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import no.ssb.vtl.model.Order;
import no.ssb.vtl.script.operations.AbstractUnaryDatasetOperation;
import no.ssb.vtl.model.Component;
import no.ssb.vtl.model.DataPoint;
import no.ssb.vtl.model.DataStructure;
import no.ssb.vtl.model.Dataset;
import no.ssb.vtl.model.VTLObject;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.*;

public class CheckSingleRuleOperation extends AbstractUnaryDatasetOperation {

    public static final String ERROR_CODE_LABEL = "errorcode";
    public static final String ERROR_LEVEL_LABEL = "errorlevel";
    public static final String CONDITION_LABEL = "condition";

    private final RowsToReturn rowsToReturn;
    private final ComponentsToReturn componentsToReturn;
    private final String errorCode;
    private final Long errorLevel;
    private final Set<Component> conditions = Sets.newHashSet();

    private CheckSingleRuleOperation(Builder builder) {
        super(checkNotNull(builder.dataset, "dataset was null"));
        this.rowsToReturn = builder.rowsToReturn;
        this.componentsToReturn = builder.componentsToReturn;
        this.errorCode = builder.errorCode;
        this.errorLevel = builder.errorLevel;

        checkArgument(!(this.componentsToReturn == ComponentsToReturn.MEASURES && this.rowsToReturn == RowsToReturn.ALL),
                "cannot use 'all' with 'measures' parameter");

        checkDataStructure(getChild());
    }

    private static boolean isConditionName(String name) {
        return CONDITION_LABEL.equals(name.toLowerCase()) || name.toLowerCase().endsWith("_condition");
    }

    private void checkDataStructure(Dataset dataset) {
        int noIdentifiers = Maps.filterValues(dataset.getDataStructure().getRoles(), role -> role == Component.Role.IDENTIFIER)
                .size();
        checkArgument(noIdentifiers > 0, "dataset does not have identifier components");
    }

    @Override
    protected Optional<Stream<DataPoint>> computeData(Order orders, Filtering filtering, Set<String> components) {
        return null;
    }

    @Override
    public Stream<DataPoint> getData() {
        Dataset childDataset = getChild();
        DataStructure newStructure = getDataStructure();
        DataStructure previousStructure = childDataset.getDataStructure();
        Component conditionComponent = getConditionComponent(newStructure);

        return childDataset.getData().map(dataPoint -> {

            DataPoint resultDataPoint = DataPoint.create(newStructure.size());
            Map<Component, VTLObject> originalMap = previousStructure.asMap(dataPoint);
            Map<Component, VTLObject> resultMap = newStructure.asMap(resultDataPoint);

            for (Component component : resultMap.keySet()) {
                if (originalMap.containsKey(component)) {
                    resultMap.put(component, originalMap.get(component));
                }
            }

            // Optimized and.
            // TODO: Handle nulls somewhere else (VTLBoolean?)
            Boolean combinedCondition = true;
            for (Component condition : conditions) {
                VTLObject value = originalMap.get(condition);
                if (value.get() == null) {
                    combinedCondition = false;
                    break;
                } else {
                    if (!(combinedCondition && (Boolean) value.get())) {
                        combinedCondition = false;
                        break;
                    }
                }
            }

            //... then filter rows
            if (rowsToReturn != RowsToReturn.ALL) {
                if (rowsToReturn == RowsToReturn.VALID && !combinedCondition) {
                    return null;
                }
                if (rowsToReturn == RowsToReturn.NOT_VALID && combinedCondition) {
                    return null;
                }
            }

            resultMap.computeIfPresent(getErrorCodeComponent(), (k, v) -> VTLObject.of(errorCode));
            resultMap.computeIfPresent(getErrorLevelComponent(), (k, v) -> VTLObject.of(errorLevel));

            Boolean finalPrevious = combinedCondition;
            resultMap.computeIfPresent(conditionComponent, (k, v) -> VTLObject.of(finalPrevious));


            return resultDataPoint;
        }).filter(Objects::nonNull);
    }

    private Component getConditionComponent(DataStructure structure) {
        return structure.get(CONDITION_LABEL);
    }

    private Component getErrorLevelComponent() {
        return getDataStructure().get(ERROR_LEVEL_LABEL);
    }

    private Component getErrorCodeComponent() {
        return getDataStructure().get(ERROR_CODE_LABEL);
    }

    @Override
    public Optional<Map<String, Integer>> getDistinctValuesCount() {
        return Optional.empty();
    }

    @Override
    public Optional<Long> getSize() {
        return Optional.empty();
    }


    @Override
    protected DataStructure computeDataStructure() {
        Dataset dataset = getChild();
        DataStructure structure = dataset.getDataStructure();

        DataStructure.Builder builder = DataStructure.builder();
        for (Map.Entry<String, Component> entry : structure.entrySet()) {
            Component component = entry.getValue();
            if (component.isIdentifier()) {
                builder.put(entry);
            } else if (component.isMeasure()) {
                if (isConditionName(entry.getKey()) && component.getType().isAssignableFrom(Boolean.class)) {
                    checkArgument(
                            conditions.add(component),
                            "duplicate condition %s in %s",
                            entry, structure
                    );
                }
                if (componentsToReturn == ComponentsToReturn.MEASURES) {
                    if (!isConditionName(entry.getKey())) {
                        builder.put(entry);
                    }
                }
            } else {
                builder.put(entry);
            }
        }

        checkArgument(!conditions.isEmpty(), "the dataset %s was not a boolean dataset", dataset);
        if (componentsToReturn == ComponentsToReturn.CONDITION) {
            builder.put(CONDITION_LABEL, Component.Role.MEASURE, Boolean.class);
        }

        builder.put(ERROR_CODE_LABEL, Component.Role.ATTRIBUTE, String.class);

        if (errorLevel != null) {
            builder.put(ERROR_LEVEL_LABEL, Component.Role.ATTRIBUTE, Long.class);
        }

        return builder.build();
    }


    public enum RowsToReturn {
        NOT_VALID,
        VALID,
        ALL
    }

    public enum ComponentsToReturn {
        MEASURES,
        CONDITION
    }

    public static class Builder {

        private Dataset dataset;
        private RowsToReturn rowsToReturn = RowsToReturn.NOT_VALID;
        private ComponentsToReturn componentsToReturn = ComponentsToReturn.MEASURES;
        private String errorCode;
        private Long errorLevel;

        public Builder(Dataset dataset) {
            this.dataset = dataset;
        }

        public Builder rowsToReturn(RowsToReturn rowsToReturn) {
            this.rowsToReturn = rowsToReturn;
            return this;
        }

        public Builder componentsToReturn(ComponentsToReturn componentsToReturn) {
            this.componentsToReturn = componentsToReturn;
            return this;
        }

        public Builder errorCode(String errorCode) {
            this.errorCode = errorCode;
            return this;
        }

        public Builder errorLevel(Long errorLevel) {
            this.errorLevel = errorLevel;
            return this;
        }

        public CheckSingleRuleOperation build() {
            return new CheckSingleRuleOperation(this);
        }
    }
}
