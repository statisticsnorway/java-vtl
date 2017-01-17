package kohl.hadrien.vtl.model;

import com.google.common.base.MoreObjects;

import java.util.Objects;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A data point is a simple reference holder for values.
 */
public abstract class DataPoint<V> implements Supplier<V> {

    private final Component componentReference;

    public DataPoint(Component component) {
        this.componentReference = checkNotNull(component);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataPoint<?> dataPoint = (DataPoint<?>) o;
        return Objects.equals(componentReference, dataPoint.componentReference) &&
                Objects.equals(get(), dataPoint.get());
    }

    @Override
    public int hashCode() {
        return Objects.hash(componentReference, get());
    }

    /**
     * Returns the value of the data point.
     */
    @Override
    public abstract V get();

    /**
     * Returns the componentReference (type and role) of this data point.
     */
    public Component getComponent() {
        return componentReference;
    }

    @Override
    public String toString() {
        String value = get() == null ? "[NULL]" : get().toString();

        return MoreObjects.toStringHelper(super.toString())
        //return MoreObjects.toStringHelper(this)
                .addValue(getType().getSimpleName())
                .addValue(getRole())
                .addValue(getName())
                .toString().concat(" = ").concat(value);
    }

    /**
     * Convenience method giving the type of the data point.
     * <p>
     * It is strictly equivalent to getComponent().getType();
     */
    public Class<?> getType() {
        return getComponent().getType();
    }

    /**
     * Convenience method returning the {@link Component.Role} of the data point.
     * <p>
     * It is strictly equivalent to getComponent().getRole();
     */
    public Component.Role getRole() {
        return getComponent().getRole();
    }

    public String getName() {
        return getComponent().getName();
    }
}
