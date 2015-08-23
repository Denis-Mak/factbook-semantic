package it.factbook.semantic;

import it.factbook.dictionary.Stem;
import it.factbook.util.BitUtils;

import java.io.Serializable;

public class SemanticKey implements Serializable {
    private int golem;
    private long[] boolVectorAsLongs;
    private String randomIndex;
    private int[] mem;
    private int weight;
    private int commonMems;

    public SemanticKey(int golem, boolean[] boolVector, int[] mem, int weight, int commonMems) {
        this.golem = golem;
        this.boolVectorAsLongs = BitUtils.convertToLongArray(boolVector);
        this.randomIndex = BitUtils.sparseVectorHash(boolVector, false);
        this.mem = mem;
        this.weight = weight;
        this.commonMems = commonMems;
    }

    public SemanticKey(int golem, String randomIndex, int[] mem, int weight, int commonMems) {
        this.golem = golem;
        this.boolVectorAsLongs = BitUtils.convertToLongArray(BitUtils.reverseHash(randomIndex, Stem.RI_VECTOR_LENGTH));
        this.randomIndex = randomIndex;
        this.mem = mem;
        this.weight = weight;
        this.commonMems = commonMems;
    }

    public int getGolem() {
        return golem;
    }

    public void setGolem(int golem) {
        this.golem = golem;
    }

    public int[] getMem() {
        return mem;
    }

    public void setMem(int[] mem) {
        this.mem = mem;
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
}
