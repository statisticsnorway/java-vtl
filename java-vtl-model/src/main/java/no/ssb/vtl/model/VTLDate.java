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

import java.time.Instant;
import java.time.Year;
import java.time.format.DateTimeFormatter;
import java.util.TimeZone;

import static java.lang.String.*;

// TODO: The spec specifies that date format should be configurable.
public abstract class VTLDate extends VTLObject<Instant> implements VTLTyped<VTLDate> {

    private VTLDate() {
        // private
    }

    @Override
    public Class<VTLDate> getVTLType() {
        return VTLDate.class;
    }

    public static VTLDate of(String input, String dateFormat, TimeZone timeZone) {

        if (!canParse(dateFormat)) {
            throw new RuntimeException(
                    format("Date format %s unsupported", dateFormat));
        }

        return new VTLDate() {

            @Override
            public Instant get() {
                DateTimeFormatter formatter =
                        DateTimeFormatter.ofPattern("yyyy");
                Year year = Year.parse(input, formatter);
                return year.atDay(1).atStartOfDay(timeZone.toZoneId()).toInstant();
            }
        };

    }

    public static VTLDate of(Instant instant) {

        return new VTLDate() {

            @Override
            public Instant get() {
                return instant;
            }
        };

    }

    public static boolean canParse(String dateFormat) {
        return "YYYY".equals(dateFormat);
    }

}
