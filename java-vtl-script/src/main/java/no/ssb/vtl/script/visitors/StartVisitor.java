package no.ssb.vtl.script.operations.visitors;

import com.google.common.collect.Lists;
import no.ssb.vtl.model.Dataset;
import no.ssb.vtl.parser.VTLBaseVisitor;
import no.ssb.vtl.parser.VTLParser;

import java.util.LinkedList;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Created by hadrien on 18/10/2016.
 */
public class StartVisitor extends VTLBaseVisitor<LinkedList<Dataset>> {

    private final AssignmentVisitor assignmentVisitor;

    public StartVisitor(AssignmentVisitor assignmentVisitor) {
        this.assignmentVisitor = checkNotNull(assignmentVisitor);
    }

    @Override
    protected LinkedList<Dataset> defaultResult() {
        return Lists.newLinkedList();
    }

    @Override
    public LinkedList<Dataset> visitStart(VTLParser.StartContext ctx) {
        for (VTLParser.StatementContext statement : ctx.statement()) {
            Dataset assigned = assignmentVisitor.visit(statement);
            defaultResult().add(assigned);
        }
        return defaultResult();
    }
}
