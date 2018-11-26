package org.apache.druid.server.grpc.common;

import gnu.trove.impl.Constants;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import gnu.trove.procedure.TObjectIntProcedure;

import java.util.ConcurrentModificationException;

/**
 * Save on data transfer volume and string comparison by dictionary encoding of String values.
 *
 * Strictly speaking a dictionary should be generated in the ETL pipeline and only used to the query engine. But
 * this is not how Druid operates. Even though every segment actually has dictionaries internally.
 */
public final class DictionaryEncoders {
    public final static int InitialCapacity = 1024;

    /** Assume the total cardinality of a tenant's dimensions is low enough to fit into a single map. */
    public interface DictionaryEncoder {
        int NULL = -1;

        int encode(String value);

        String decode(int encoded);

        int size();
    }

    public static DictionaryEncoder dictionaryEncoder() {
        return new TroveDictionaryEncoder(InitialCapacity);
    }
    
    static final class TroveDictionaryEncoder implements DictionaryEncoder {
        private final TObjectIntMap<String> strToInt;
        private final TIntObjectMap<String> intToStr;

        public TroveDictionaryEncoder(int initialCapacity) {
            strToInt = createMap(initialCapacity);
            intToStr = new TIntObjectHashMap<>(initialCapacity, Constants.DEFAULT_LOAD_FACTOR, NULL);
        }

        public TroveDictionaryEncoder(TObjectIntMap<String> strToInt) {
            this.strToInt = strToInt;

            intToStr = new TIntObjectHashMap<>(strToInt.size(), Constants.DEFAULT_LOAD_FACTOR, NULL);
            strToInt.forEachEntry((name, code) -> {intToStr.put(code, name); return true;});
        }

        @Override
        public int encode(String value) {
            final int existingKey = strToInt.get(value);

            if (existingKey == NULL) {
                final int key = strToInt.size();
                if (strToInt.put(value, key) != NULL) {
                    throw new ConcurrentModificationException(value + " is already known as " + key);
                }

                if (intToStr.put(key, value) != null) {
                    throw new ConcurrentModificationException("Key is already used: " + key);
                }

                return key;
            } else {
                return existingKey;
            }
        }

        @Override
        public String decode(int encoded) {
            final String decoded = intToStr.get(encoded);

            if (decoded == null) {
                throw new IllegalArgumentException("Unknown dimension value: " + encoded);
            }

            return decoded;
        }

        @Override
        public int size() {
            return intToStr.size();
        }

        public boolean traverse(TObjectIntProcedure<String> visitor) {
            return strToInt.forEachEntry(visitor);
        }

        public static TObjectIntMap<String> createMap(int size) {
            return new TObjectIntHashMap<>(size, Constants.DEFAULT_LOAD_FACTOR, NULL);
        }
    }
}
