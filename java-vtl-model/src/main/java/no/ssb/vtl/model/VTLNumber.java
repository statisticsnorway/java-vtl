package no.ssb.vtl.model;

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

import static java.lang.String.*;

public abstract class VTLNumber<T extends Number> extends VTLObject<T> {

    VTLNumber() {
        // private.
    }

    @Override
    public abstract T get();

    @Override
    public int compareTo(Object o) {
        if (o instanceof VTLNumber) {
            VTLNumber that = ((VTLNumber) o);
            return Double.compare(this.get().doubleValue(), that.get().doubleValue());
        } else {
            return super.compareTo(o);
        }
    }

    public VTLNumber add(VTLNumber addend) {
        return add(addend.get());
    }
    
    public VTLNumber add(Number addend) {
        Number augend = get();
        if (augend instanceof Double || addend instanceof Double) {
                return VTLNumber.of(augend.doubleValue() + addend.doubleValue());
        } else if (augend instanceof Long || addend instanceof Long) {
                return VTLNumber.of(augend.longValue() + addend.longValue());
        }
    
        throw new RuntimeException(
                format("unsupported number types %s, %s", augend.getClass(), addend.getClass())
        );
        
    }
    
    public VTLNumber subtract(VTLNumber subtrahend) {
        return subtract((Number) subtrahend.get());
    }
    
    public VTLNumber subtract(Number subtrahend) {
        Number minuend = get();
        if (minuend instanceof Double || subtrahend instanceof Double) {
            return VTLNumber.of(minuend.doubleValue() - subtrahend.doubleValue());
        } else if (minuend instanceof Long || subtrahend instanceof Long) {
            return VTLNumber.of(minuend.longValue() - subtrahend.longValue());
        }
        
        throw new RuntimeException(
                format("unsupported number types %s, %s", minuend.getClass(), subtrahend.getClass())
        );
    }
    
    public VTLNumber multiply(VTLNumber multiplicand) {
        return multiply((Number) multiplicand.get());
    }
    
    public VTLNumber multiply(Number multiplicand) {
        Number multiplier = get();
        if (multiplier instanceof Double || multiplicand instanceof Double) {
            return VTLNumber.of(multiplier.doubleValue() * multiplicand.doubleValue());
        } else if (multiplier instanceof Long || multiplicand instanceof Long) {
            return VTLNumber.of(multiplier.longValue() * multiplicand.longValue());
        }
        throw new RuntimeException(format("unsupported number types %s, %s", multiplier.getClass(), multiplicand.getClass()));
    }
    
    public VTLNumber divide(VTLNumber divisor) {
        return divide((Number) divisor.get());
    }
    
    public VTLNumber divide(Number divisor) {
        if (divisor.doubleValue() == 0) {
            throw new IllegalArgumentException("Cannot divide by zero");
        }

        Number dividend = get();
        return VTLNumber.of(dividend.doubleValue() / divisor.doubleValue());
    }
}
