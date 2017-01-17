package no.ssb.vtl.script.join;

import com.google.common.collect.Sets;
import no.ssb.vtl.model.Dataset;
import no.ssb.vtl.parser.VTLBaseVisitor;
import no.ssb.vtl.parser.VTLParser;
import no.ssb.vtl.script.operations.DropOperator;

import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Visit the drop clauses.
 */
public class JoinDropClauseVisitor extends VTLBaseVisitor<DropOperator> {

    private final Dataset dataset;

    public JoinDropClauseVisitor(Dataset dataset) {
        this.dataset = checkNotNull(dataset, "dataset was null");
    }

    @Override
    public DropOperator visitJoinDropExpression(VTLParser.JoinDropExpressionContext ctx) {
        Set<String> components = Sets.newHashSet();
        for (VTLParser.JoinDropRefContext joinDropRefContext : ctx.joinDropRef()) {
            components.add(joinDropRefContext.getText());
        }
        return new DropOperator(dataset, components);
    }
}
