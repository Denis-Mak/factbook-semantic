package it.factbook.semantic;

import java.io.Serializable;

/**
 * Container of one row of the table semantic_index_v2 it is more convenient way to use beans with Cassandra driver for Spark
 * then a mapper.
 */
public class SemanticIndexRow implements Serializable {
    private int golem;
    private String randomIndex;
    private String mem;
    private String url;
    private int pos;
    private int factuality;
    private int fingerprint;
    private String idiom;
    private int frequency;

    public int getGolem() {
        return golem;
    }

    public void setGolem(int golem) {
        this.golem = golem;
    }

    public String getRandomIndex() {
        return randomIndex;
    }

    public void setRandomIndex(String randomIndex) {
        this.randomIndex = randomIndex;
    }

    public String getMem() {
        return mem;
    }

    public void setMem(String mem) {
        this.mem = mem;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public int getPos() {
        return pos;
    }

    public void setPos(int pos) {
        this.pos = pos;
    }

    public int getFactuality() {
        return factuality;
    }

    public void setFactuality(int factuality) {
        this.factuality = factuality;
    }

    public int getFingerprint() {
        return fingerprint;
    }

    public void setFingerprint(int fingerprint) {
        this.fingerprint = fingerprint;
    }

    public String getIdiom() {
        return idiom;
    }

    public void setIdiom(String idiom) {
        this.idiom = idiom;
    }

    public int getFrequency() {
        return frequency;
    }

    public void setFrequency(int frequency) {
        this.frequency = frequency;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SemanticIndexRow that = (SemanticIndexRow) o;

        if (golem != that.golem) return false;
        if (pos != that.pos) return false;
        if (factuality != that.factuality) return false;
        if (fingerprint != that.fingerprint) return false;
        if (!randomIndex.equals(that.randomIndex)) return false;
        if (!mem.equals(that.mem)) return false;
        if (!url.equals(that.url)) return false;
        return idiom.equals(that.idiom);

    }

    @Override
    public int hashCode() {
        int result = golem;
        result = 31 * result + randomIndex.hashCode();
        result = 31 * result + mem.hashCode();
        result = 31 * result + url.hashCode();
        result = 31 * result + pos;
        result = 31 * result + factuality;
        result = 31 * result + fingerprint;
        result = 31 * result + idiom.hashCode();
        return result;
    }
}
