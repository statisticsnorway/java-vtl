package no.ssb.vtl.script.visitors.join;

import no.ssb.vtl.model.DataStructure;
import no.ssb.vtl.model.Dataset;

import java.util.function.UnaryOperator;

/**
 * A join clause can be applied to a {@link DataStructure}
 * and {@link Dataset.Tuple}.
 * <p>
 * Join clauses have access to a special "working dataset" that can be mutated
 * during the join.
 */
public interface JoinClause extends UnaryOperator<WorkingDataset> {
}
