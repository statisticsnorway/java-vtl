package no.ssb.vtl.script.join;

import com.google.common.collect.ImmutableMap;
import no.ssb.vtl.model.Component;
import no.ssb.vtl.model.DataStructure;
import no.ssb.vtl.model.Dataset;
import no.ssb.vtl.parser.VTLBaseVisitor;
import no.ssb.vtl.parser.VTLParser;
import no.ssb.vtl.script.operations.RenameOperation;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A visitor that instantiate rename operators.
 */
public class JoinRenameClauseVisitor extends VTLBaseVisitor<RenameOperation> {

    private final Dataset dataset;

    public JoinRenameClauseVisitor(WorkingDataset dataset) {
        this.dataset = checkNotNull(dataset);
    }

    @Override
    public RenameOperation visitJoinRenameExpression(VTLParser.JoinRenameExpressionContext ctx) {
        DataStructure dataStructure = dataset.getDataStructure();
        ImmutableMap.Builder<String, String> newNames = ImmutableMap.builder();
        ImmutableMap.Builder<String, Component.Role> newRoles = ImmutableMap.builder();
        for (VTLParser.JoinRenameParameterContext renameParam : ctx.joinRenameParameter()) {
            String from = renameParam.from.getText();
            String to = renameParam.to.getText();
            newNames.put(from, to);
            checkArgument(
                    dataStructure.containsKey(from),
                    "could not find component with name %s",
                    from
                    );
            newRoles.put(from, dataStructure.get(from).getRole());
        }
        return new RenameOperation(dataset, newNames.build(), newRoles.build());
    }
}
