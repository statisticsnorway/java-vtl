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

import com.google.common.collect.ImmutableMap;
import no.ssb.vtl.model.Component;
import no.ssb.vtl.model.Dataset;
import no.ssb.vtl.parser.VTLBaseVisitor;
import no.ssb.vtl.parser.VTLParser;
import no.ssb.vtl.script.operations.rename.RenameOperation;
import no.ssb.vtl.script.operations.join.ComponentBindings;

import java.util.List;
import java.util.function.Function;

/**
 * A visitor that handles the getClauses.
 */
public class ClauseVisitor extends VTLBaseVisitor<Function<Dataset, Dataset>> {

    @Override
    protected Function<Dataset, Dataset> defaultResult() {
        return Function.identity();
    }

    @Override
    protected Function<Dataset, Dataset> aggregateResult(Function<Dataset, Dataset> aggregate,
                                                         Function<Dataset, Dataset> nextResult) {
        return aggregate.andThen(nextResult);
    }

    @Override
    public Function<Dataset, Dataset> visitRenameClause(VTLParser.RenameClauseContext ctx) {
        return dataset -> {

            ComponentBindings bindings = new ComponentBindings(dataset);
            ComponentVisitor componentVisitor = new ComponentVisitor(bindings);

            List<VTLParser.RenameParamContext> parameters = ctx.renameParam();

            ImmutableMap.Builder<String, String> names = ImmutableMap.builder();
            ImmutableMap.Builder<String, Component.Role> roles = ImmutableMap.builder();

            ComponentRoleVisitor roleVisitor = ComponentRoleVisitor.getInstance();

            for (VTLParser.RenameParamContext parameter : parameters) {
                // Check is defined.
                componentVisitor.visit(parameter.from);
                String to = parameter.to.getText();
                names.put(parameter.from.getText(), to);

                if (parameter.role != null) {
                    Component.Role role = roleVisitor.visit(parameter.role);
                    roles.put(parameter.from.getText(), role);
                }
            }

            return new RenameOperation(dataset, names.build(), roles.build());
        };
    }
}
