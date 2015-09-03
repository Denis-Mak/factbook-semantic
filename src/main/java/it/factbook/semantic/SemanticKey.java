package it.factbook.semantic;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import it.factbook.dictionary.Stem;
import it.factbook.util.BitUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Arrays;

public class SemanticKey implements Serializable {
    private static final ObjectMapper jsonMapper = new ObjectMapper();
    private static final Logger log = LoggerFactory.getLogger(SemanticKey.class);
    private int golem;
    private long[] boolVectorAsLongs;
    // Use String attributes randomIndex and mem because of restrictions of joinWithCassandraTable method
    // Where attribute name must be equal column name
    private String randomIndex;
    private String mem;
    private int[] memIntArr;
    private int weight;
    private int commonMems;

    public SemanticKey(int golem, boolean[] boolVector, int[] mem, int weight, int commonMems) {
        this.golem = golem;
        this.boolVectorAsLongs = BitUtils.convertToLongArray(boolVector);
        this.randomIndex = BitUtils.sparseVectorHash(boolVector, false);
        try {
            this.mem = jsonMapper.writeValueAsString(mem);
        } catch (JsonProcessingException e) {
            log.error("Exeption in parsing mem");
        }
        this.memIntArr = mem;
        this.weight = weight;
        this.commonMems = commonMems;
    }

    public SemanticKey(int golem, String randomIndex, int[] mem, int weight, int commonMems) {
        this.golem = golem;
        this.boolVectorAsLongs = BitUtils.convertToLongArray(BitUtils.reverseHash(randomIndex, Stem.RI_VECTOR_LENGTH));
        this.randomIndex = randomIndex;
        try {
            this.mem = jsonMapper.writeValueAsString(mem);
        } catch (JsonProcessingException e) {
            log.error("Exeption in parsing mem");
        }
        this.memIntArr = mem;
        this.weight = weight;
        this.commonMems = commonMems;
    }

    public int getGolem() {
        return golem;
    }

    public void setGolem(int golem) {
        this.golem = golem;
    }

    public String getMem() {
        return mem;
    }

    public void setMem(String mem) {
        this.mem = mem;
    }

    public int[] getMemIntArr() {
        return memIntArr;
    }

    public void setMemIntArr(int[] memIntArr) {
        this.memIntArr = memIntArr;
    }

    public int getWeight() {
        return weight;
    }

    public void setWeight(int weight) {
        this.weight = weight;
    }

    public int getCommonMems() {
        return commonMems;
    }

    public void setCommonMems(int commonMems) {
        this.commonMems = commonMems;
    }

    public String getRandomIndex() {
        return randomIndex;
    }

    public void setRandomIndex(String randomIndex) {
        this.randomIndex = randomIndex;
    }

    public long[] getBoolVectorAsLongs() {
        return boolVectorAsLongs;
    }

    public void setBoolVectorAsLongs(long[] boolVectorAsLongs) {
        this.boolVectorAsLongs = boolVectorAsLongs;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SemanticKey that = (SemanticKey) o;

        if (golem != that.golem) return false;
        if (weight != that.weight) return false;
        if (commonMems != that.commonMems) return false;
        if (!randomIndex.equals(that.randomIndex)) return false;
        return mem.equals(that.mem);

    }

    @Override
    public int hashCode() {
        int result = golem;
        result = 31 * result + randomIndex.hashCode();
        result = 31 * result + mem.hashCode();
        result = 31 * result + weight;
        result = 31 * result + commonMems;
        return result;
    }
}
