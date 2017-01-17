package kohl.hadrien.vtl.script.support;

import com.google.common.collect.Lists;

import java.util.AbstractList;
import java.util.Iterator;
import java.util.List;
import java.util.RandomAccess;

import static com.google.common.base.Preconditions.checkNotNull;

public class CombinedList<T> extends AbstractList<T> implements RandomAccess {

    final List<List<T>> lists = Lists.newArrayList();
    int size = 0;

    public CombinedList(List<T> first, List<T>... others) {
        this.lists.add(first);
        for (List<T> list : others) {
            this.lists.add(list);
        }
        computeSize();
    }

    public CombinedList(List<List<T>> lists) {
        this.lists.addAll(checkNotNull(lists));
        computeSize();
    }

    private void computeSize() {
        size = 0;
        for (List<T> list : lists) {
            // TODO: Check for overflow.
            size += list.size();
        }
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public T get(int index) {
        for (List<T> list : lists) {
            if (index < list.size())
                return list.get(index);
            index -= list.size();
        }
        throw new IndexOutOfBoundsException();
    }

    @Override
    public boolean add(T t) {
        boolean added = lists.get(lists.size() - 1).add(t);
        computeSize();
        return added;
    }

    @Override
    public T remove(int index) {
        for (List<T> list : lists) {
            if (index < list.size()) {
                T removed = list.remove(index);
                computeSize();
                return removed;
            }
            index -= list.size();
        }
        throw new IndexOutOfBoundsException();
    }

    @Override
    public Iterator<T> iterator() {
        return new Iterator<T>() {

            int pos = -1;
            @Override
            public boolean hasNext() {
                return pos + 1 < size();
            }

            @Override
            public T next() {
                return CombinedList.this.get(++pos);
            }

            @Override
            public void remove() {
                CombinedList.this.remove(pos);
            }
        };
    }

    @Override
    public T set(int index, T element) {
        for (List<T> list : lists) {
            if (index < list.size())
                return list.set(index, element);
            index -= list.size();
        }
        throw new IndexOutOfBoundsException();
    }
}
