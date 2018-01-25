package no.ssb.vtl.script.error;

/*
 * ========================LICENSE_START=================================
 * Java VTL
 * %%
 * Copyright (C) 2016 - 2017 Hadrien Kohl
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *      http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * =========================LICENSE_END==================================
 */

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class VTLScriptExceptionTest {

    @Test
    public void testContextWithStartStop() throws Exception {

        ParserRuleContext context = mock(ParserRuleContext.class);
        Token start = mock(Token.class);
        Token stop = mock(Token.class);

        when(start.getCharPositionInLine()).thenReturn(1);
        when(stop.getCharPositionInLine()).thenReturn(2);

        when(start.getLine()).thenReturn(3);
        when(stop.getLine()).thenReturn(4);

        when(context.getStart()).thenReturn(start);
        when(context.getStop()).thenReturn(stop);

        VTLScriptException exception = new VTLScriptException("user defined message", context);

        assertThat(exception.getStartColumn()).isEqualTo(1);
        assertThat(exception.getStopColumn()).isEqualTo(2);
        assertThat(exception.getStartLine()).isEqualTo(3);
        assertThat(exception.getStopLine()).isEqualTo(4);

    }

    @Test
    public void testContextWithSameStartStop() throws Exception {

        ParserRuleContext context = mock(ParserRuleContext.class);
        Token token = mock(Token.class);

        when(token.getCharPositionInLine()).thenReturn(0);
        when(token.getLine()).thenReturn(0);

        when(context.getStart()).thenReturn(token);
        when(context.getStop()).thenReturn(token);

        when(token.getText()).thenReturn("token");

        VTLScriptException exception = new VTLScriptException("user defined message", context);

        assertThat(exception.getStartColumn()).isEqualTo(0);
        assertThat(exception.getStopColumn()).isEqualTo(5);
        assertThat(exception.getStartLine()).isEqualTo(0);
        assertThat(exception.getStopLine()).isEqualTo(0);

    }
}
