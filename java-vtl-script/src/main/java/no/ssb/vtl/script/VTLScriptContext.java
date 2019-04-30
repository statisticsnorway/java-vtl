package no.ssb.vtl.script;

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

import javax.script.Bindings;
import javax.script.SimpleBindings;
import javax.script.SimpleScriptContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class VTLScriptContext extends SimpleScriptContext {
    
    private Map<Integer, Bindings> scopes;
    
    @SuppressWarnings("WeakerAccess")
    public VTLScriptContext() {
        super();
        engineScope = new SimpleBindings(Maps.newLinkedHashMap());
        scopes = new HashMap<>(2);
        scopes.put(ENGINE_SCOPE, engineScope);
        scopes.put(GLOBAL_SCOPE, new SimpleBindings(Maps.newLinkedHashMap()));
    }
    
    public void addScope(int scope) {
        scopes.put(scope, new SimpleBindings(Maps.newLinkedHashMap()));
    }
    
    
    
    /**
     * Associates a <code>Bindings</code> instance with a particular scope in this
     * <code>ScriptContext</code>.  Calls to the <code>getAttribute</code> and
     * <code>setAttribute</code> methods must map to the <code>get</code> and
     * <code>put</code> methods of the <code>Bindings</code> for the specified scope.
     * If the scope does not already exists in this <code>ScriptContext</code> it will be added
     * @param bindings The <code>Bindings</code> to associate with the given scope
     * @param scope The scope
     * @throws NullPointerException if the specified <code>Bindings</code> is null.
     */
    @Override
    public void setBindings(Bindings bindings, int scope) {
        if (bindings == null) {
            throw new NullPointerException("Bindings for a scope cannot be null");
        }
        scopes.put(scope, bindings);
    }
    
    /**
     * Gets the <code>Bindings</code>  associated with the given scope in this
     * <code>ScriptContext</code>.
     * @param scope The scope
     * @return The associated <code>Bindings</code>.  Returns <code>null</code> if it has not
     * been set.
     * @throws IllegalArgumentException If no <code>Bindings</code> is defined for the
     * specified scope value in <code>ScriptContext</code> of this type.
     */
    @Override
    public Bindings getBindings(int scope) {
        checkScope(scope);
        return scopes.get(scope);
    }
    
    /**
     * Sets the value of an attribute in a given scope.
     * @param name The name of the attribute to set
     * @param value The value of the attribute
     * @param scope The scope in which to set the attribute
     * @throws IllegalArgumentException if the name is empty or if the scope is invalid.
     * @throws NullPointerException if the name is null.
     */
    @Override
    public void setAttribute(String name, Object value, int scope) {
        checkName(name);
        checkScope(scope);
        scopes.get(scope).put(name, value);
    }
    
    /**
     * Sets the value of an attribute in the default engine scope.
     * @param name The name of the attribute to set
     * @param value The value of the attribute
     * @throws IllegalArgumentException if the name is empty.
     * @throws NullPointerException if the name is null.
     */
    public void setAttribute(String name, Object value) {
        setAttribute(name, value, ENGINE_SCOPE);
    }
    
    /**
     * Gets the value of an attribute in a given scope.
     * @param name The name of the attribute to retrieve.
     * @param scope The scope in which to retrieve the attribute.
     * @return The value of the attribute. Returns <code>null</code> is the name
     * does not exist in the given scope.
     * @throws IllegalArgumentException if the name is empty or if the value of scope is invalid.
     * @throws NullPointerException if the name is null.
     */
    @Override
    public Object getAttribute(String name, int scope) {
        checkName(name);
        checkScope(scope);
        return scopes.get(scope).get(name);
    }
    
    /**
     * Remove an attribute in a given scope.
     * @param name The name of the attribute to remove
     * @param scope The scope in which to remove the attribute
     * @return The removed value.
     * @throws IllegalArgumentException if the name is empty or if the scope is invalid.
     * @throws NullPointerException if the name is null.
     */
    @Override
    public Object removeAttribute(String name, int scope) {
        checkScope(scope);
        return scopes.get(scope).remove(name);
    }
    
    /**
     * Retrieves the value of the attribute with the given name in
     * the scope occurring earliest in the search order.  The order
     * is determined by the numeric value of the scope parameter (lowest
     * scope values first.)
     * @param name The name of the the attribute to retrieve.
     * @return The value of the attribute in the lowest scope for
     * which an attribute with the given name is defined.  Returns
     * null if no attribute with the name exists in any scope.
     * @throws NullPointerException if the name is null.
     * @throws IllegalArgumentException if the name is empty.
     */
    @Override
    public Object getAttribute(String name) {
        checkName(name);
        for (int scope : getScopes()) {
            Bindings bindings = scopes.get(scope);
            if (bindings.containsKey(name)) {
                return bindings.get(name);
            }
        }
        return null;
    }
    
    /**
     * Get the lowest scope in which an attribute is defined.
     * @param name Name of the attribute
     * .
     * @return The lowest scope.  Returns -1 if no attribute with the given
     * name is defined in any scope.
     * @throws NullPointerException if name is null.
     * @throws IllegalArgumentException if name is empty.
     */
    @Override
    public int getAttributesScope(String name) {
        checkName(name);
        for (int scope : getScopes()) {
            Bindings bindings = scopes.get(scope);
            if (bindings.containsKey(name)) {
                return scope;
            }
        }
        return -1;
    }
    
    /**
     * Returns immutable <code>List</code> of all the valid values for
     * scope in the ScriptContext.
     * @return list of scope values
     */
    @Override
    public List<Integer> getScopes() {
        List<Integer> scopeList = new ArrayList<>(scopes.keySet());
        Collections.sort(scopeList);
        return Collections.unmodifiableList(scopeList);
    }
    
    private void checkName(String name) {
        Objects.requireNonNull(name);
        if (name.isEmpty()) {
            throw new IllegalArgumentException("name cannot be empty");
        }
    }
    
    private void checkScope(int scope) {
        if (!scopes.containsKey(scope)) {
            throw new IllegalArgumentException("Illegal scope value");
        }
    }
    
}
