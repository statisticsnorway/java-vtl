package kohl.hadrien.vtl.script.operations;

import no.ssb.vtl.model.DataPoint;
import no.ssb.vtl.model.DataStructure;
import no.ssb.vtl.model.Dataset;

import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Addition and subtraction operation
 * TODO: Use join.
 */
public class SumOperation implements BinaryOperator<Dataset.Tuple>, Supplier<DataStructure> {

    private final Function<Dataset.Tuple, DataPoint> leftExtractor;
    private final DataStructure leftDataStructure;
    private final Function<Dataset.Tuple, DataPoint> rightExtractor;
    private final DataStructure rightDataStructure;
    private final Number scalar;

    public SumOperation(Function<Dataset.Tuple, DataPoint> leftExtractor,
                        DataStructure leftDataStructure,
                        Function<Dataset.Tuple, DataPoint> rightExtractor,
                        DataStructure rightDataStructure
    ) {
        this.leftExtractor = checkNotNull(leftExtractor);
        this.leftDataStructure = checkNotNull(leftDataStructure);
        this.rightExtractor = checkNotNull(rightExtractor);
        this.rightDataStructure = checkNotNull(rightDataStructure);
        this.scalar = null;
    }

    public SumOperation(Function<Dataset.Tuple, DataPoint> extractor, DataStructure dataStructure, Number scalar) {
        this.leftExtractor = checkNotNull(extractor);
        this.leftDataStructure = checkNotNull(dataStructure);
        this.rightExtractor = null;
        this.rightDataStructure = null;
        this.scalar = checkNotNull(scalar);
    }

    // TODO: Candidate for abstraction?
    BinaryOperator<DataStructure> getDataStructureOperator() {
        return (dataStructure, dataStructure2) -> dataStructure;
    }

    // TODO: Candidate for abstraction?
    BinaryOperator<Dataset.Tuple> getTupleOperator() {
        return new BinaryOperator<Dataset.Tuple>() {
            @Override
            public Dataset.Tuple apply(Dataset.Tuple dataPoints, Dataset.Tuple dataPoints2) {
                return null;
            }
        };
    }

    public DataStructure getDataStructure() {
        if (scalar != null) {
            return leftDataStructure;
        }
        return null;
    }


    @Override
    public Dataset.Tuple apply(Dataset.Tuple left, Dataset.Tuple right) {
        DataPoint leftValue = leftExtractor.apply(left);
        if (scalar != null) {
            return left;
        } else {
            return null;
        }
    }

    @Override
    public DataStructure get() {
        return null;
    }

    enum Op {
        ADDITION,
        SUBSTRACTION
    }

}
