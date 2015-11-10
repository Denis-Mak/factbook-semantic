package it.factbook.semantic;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import it.factbook.dictionary.Stem;
import it.factbook.util.BitUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;

/**
 * Container to keep idiom's properties used for searching in memory
 */
public class SemanticVector implements Serializable{
    private static final ObjectMapper jsonMapper = new ObjectMapper();
    static {
        jsonMapper.registerModule(new JodaModule());
    }
    private static final Logger log = LoggerFactory.getLogger(SemanticVector.class);

    private long[] boolVector;
    private int[] mem;

    public SemanticVector(){}

    public SemanticVector(String randomIndex, String mem) {
        this.boolVector = BitUtils.convertToLongArray(BitUtils.reverseHash(randomIndex, Stem.RI_VECTOR_LENGTH));
        try {
            this.mem = jsonMapper.readValue(mem, new TypeReference<int[]>() {});
        } catch (IOException e) {
            log.error("Mem {} deserialization error: {}", mem, e);
        }
    }

    public long[] getBoolVector() {
        return boolVector;
    }

    public void setBoolVector(long[] boolVector) {
        this.boolVector = boolVector;
    }

    public int[] getMem() {
        return mem;
    }

    public void setMem(int[] mem) {
        this.mem = mem;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SemanticVector that = (SemanticVector) o;

        return Arrays.equals(boolVector, that.boolVector) &&
                Arrays.equals(mem, that.mem);

    }

    @Override
    public int hashCode() {
        int result = 0;
        result = 31 * result + Arrays.hashCode(boolVector);
        result = 31 * result + Arrays.hashCode(mem);
        return result;
    }


}
