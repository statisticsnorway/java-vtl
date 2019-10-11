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

import no.ssb.vtl.model.Component;
import no.ssb.vtl.parser.VTLBaseVisitor;
import no.ssb.vtl.parser.VTLParser;

public final class ComponentRoleVisitor extends VTLBaseVisitor<Component.Role> {

    private static ComponentRoleVisitor instance;

    public static ComponentRoleVisitor getInstance() {
        if (instance == null)
            instance = new ComponentRoleVisitor();
        return instance;
    }

    private ComponentRoleVisitor() {
        // singleton.
    }

    @Override
    public Component.Role visitComponentRole(VTLParser.ComponentRoleContext ctx) {
        if (ctx == null || ctx.role == null)
            return null;

        switch (ctx.role.getType()) {
            case VTLParser.IDENTIFIER:
                return Component.Role.IDENTIFIER;
            case VTLParser.MEASURE:
                return Component.Role.MEASURE;
            case VTLParser.ATTRIBUTE:
                return Component.Role.ATTRIBUTE;
            default:
                throw new RuntimeException("unknown component type " + ctx.getText());
        }
    }
}
