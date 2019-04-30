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
import no.ssb.vtl.script.operations.rename.RenameOperation;
import no.ssb.vtl.script.visitors.ComponentVisitor;
import no.ssb.vtl.script.visitors.VTLDatasetExpressionVisitor;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A visitor that instantiate rename operators.
 */
public class RenameVisitor extends VTLDatasetExpressionVisitor<RenameOperation> {

    private final Dataset dataset;
    private final ComponentVisitor componentVisitor;

    @Deprecated
    public RenameVisitor(Dataset dataset) {
        this.dataset = checkNotNull(dataset); this.componentVisitor = null;
    }

    public RenameVisitor(Dataset dataset, ComponentVisitor componentVisitor) {
        this.dataset = checkNotNull(dataset);
        this.componentVisitor = checkNotNull(componentVisitor);
    }

    @Override
    public RenameOperation visitJoinRenameExpression(VTLParser.JoinRenameExpressionContext ctx) {
        ImmutableMap.Builder<String, String> newNames = ImmutableMap.builder();
        for (VTLParser.JoinRenameParameterContext renameParam : ctx.joinRenameParameter()) {
            Component component = componentVisitor.visit(renameParam.from);
            String actualName = dataset.getDataStructure().getName(component);
            String to = removeQuoteIfNeeded(renameParam.to.getText());
            newNames.put(actualName, to);
        }
        return new RenameOperation(dataset, newNames.build());
    }

    private static String removeQuoteIfNeeded(String key) {
        if (!key.isEmpty() && key.length() > 3) {
            if (key.charAt(0) == '\'' && key.charAt(key.length() - 1) == '\'') {
                return key.substring(1, key.length() - 1);
            }
        }
        return key;
    }
}
