package kohl.hadrien.vtl.model;

/*-
 * #%L
 * java-vtl-model
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

import com.google.common.base.MoreObjects;

import java.util.Objects;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Component represent values of a dataset.
 */
public class Component {

    private final Class<?> type;
    private final Role role;
    private final String name;

    // Use data structure static methods to create component.
    Component(Class<?> type, Role role, String name) {
        this.type = checkNotNull(type);
        this.role = checkNotNull(role);
        this.name = checkNotNull(name);
    }

    public Class<?> getType() {
        return type;
    }

    public Role getRole() {
        return role;
    }

    public String getName() {
        return name;
    }

    public boolean isIdentifier() {
        return getRole() == Role.IDENTIFIER;
    }

    public boolean isMeasure() {
        return getRole() == Role.MEASURE;
    }

    public boolean isAttribute() {
        return getRole() == Role.ATTRIBUTE;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Component component = (Component) o;
        return Objects.equals(getType(), component.getType()) &&
                Objects.equals(getName(), component.getName()) &&
                getRole() == component.getRole();
    }


    @Override
    public int hashCode() {
        return Objects.hash(getType(), getName(), getRole());
    }


    @Override
    public String toString() {
        return MoreObjects.toStringHelper(name)
                .addValue(type)
                .addValue(role)
                .toString();
    }

    public enum Role {
        IDENTIFIER,
        MEASURE,
        ATTRIBUTE
    }
}
