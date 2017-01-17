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

import com.google.common.collect.ImmutableMap;
import no.ssb.vtl.parser.VTLBaseVisitor;
import no.ssb.vtl.parser.VTLParser;
import no.ssb.vtl.script.operations.RenameOperation;
import no.ssb.vtl.model.Dataset;
import no.ssb.vtl.model.Component;


import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static java.util.Optional.ofNullable;

/**
 * A visitor that handles the clauses.
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
        List<VTLParser.RenameParamContext> parameters = ctx.renameParam();

        ImmutableMap.Builder<String, String> names = ImmutableMap.builder();
        ImmutableMap.Builder<String, Component.Role> roles = ImmutableMap.builder();

        for (VTLParser.RenameParamContext parameter : parameters) {
            String from = parameter.from.getText();
            String to = parameter.to.getText();
            names.put(from, to);

            Optional<String> role = ofNullable(parameter.role()).map(VTLParser.RoleContext::getText);
            if (role.isPresent()) {
                Component.Role roleEnum;
                switch (role.get()) {
                    case "IDENTIFIER":
                        roleEnum = Component.Role.IDENTIFIER;
                        break;
                    case "MEASURE":
                        roleEnum = Component.Role.MEASURE;
                        break;
                    case "ATTRIBUTE":
                        roleEnum = Component.Role.ATTRIBUTE;
                        break;
                    default:
                        throw new RuntimeException("unknown component type " + role.get());
                }
                roles.put(from, roleEnum);
            }
        }

        return dataset -> new RenameOperation(dataset, names.build(), roles.build());
    }
}
