/**
 * Copyright (C) 2014 Luka Obradovic.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.hoshi.tut.util.collect;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author Luka Obradovic (obradovic.luka.83@gmail.com)
 */
public class CountSet<T> {
    public static final Logger log = LoggerFactory.getLogger(CountSet.class);

    private Map<T, MutableInt> counts;

    public CountSet() {
        counts = new HashMap<T, MutableInt>();
    }

    public void add(final T data) {
        MutableInt count = counts.get(data);

        if (count == null) {
            count = new MutableInt();
            counts.put(data, count);
        }

        count.increment();
    }

    public int count(final T data) {
        final MutableInt count = counts.get(data);

        if (count == null) {
            return 0;
        } else {
            return count.get();
        }
    }

    public Set<T> data() {
        return counts.keySet();
    }

    private static class MutableInt {
        private int value = 0;

        public int get() { return value; }

        public void increment() { ++value; }
    }
}