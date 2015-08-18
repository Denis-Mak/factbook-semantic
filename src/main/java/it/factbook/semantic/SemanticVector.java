package it.factbook.semantic;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
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

public class SemanticVector implements Serializable, KryoSerializable {
    private static final ObjectMapper jsonMapper = new ObjectMapper();
    static {
        jsonMapper.registerModule(new JodaModule());
    }
    private static final Logger log = LoggerFactory.getLogger(SemanticVector.class);

    private int golem;
    private boolean[] boolVector;
    private int[] mem;

    public SemanticVector(){}

    public SemanticVector(int golem, boolean[] boolVector, int[] mem) {
        this.golem = golem;
        this.boolVector = boolVector;
        this.mem = mem;
    }

    public SemanticVector(int golem, String randomIndex, String mem) {
        this.golem = golem;
        this.boolVector = BitUtils.reverseHash(randomIndex, Stem.RI_VECTOR_LENGTH);
        try {
            this.mem = jsonMapper.readValue(mem, new TypeReference<int[]>() {});
        } catch (IOException e) {
            log.error("Mem {} deserialization error: {}", mem, e);
        }
    }

    public int getGolem() {
        return golem;
    }

    public void setGolem(int golem) {
        this.golem = golem;
    }

    public boolean[] getBoolVector() {
        return boolVector;
    }

    public void setBoolVector(boolean[] boolVector) {
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

        return golem == that.golem && Arrays.equals(boolVector, that.boolVector) &&
                Arrays.equals(mem, that.mem);

    }

    @Override
    public int hashCode() {
        int result = golem;
        result = 31 * result + Arrays.hashCode(boolVector);
        result = 31 * result + Arrays.hashCode(mem);
        return result;
    }

    @Override
    public void write(Kryo kryo, Output output) {
        output.writeByte(golem);
        byte[] bytes = BitUtils.convertToByteArray(boolVector);
        for (byte each: bytes){
            output.writeByte(each);
        }
        for (int each: mem){
            output.writeInt(each);
        }
    }

    @Override
    public void read(Kryo kryo, Input input) {
        golem = input.readByte();
        boolVector = new boolean[Stem.RI_VECTOR_LENGTH];
        final int bytesLen = Stem.RI_VECTOR_LENGTH / (Byte.SIZE - 1) +
                ((Stem.RI_VECTOR_LENGTH % (Byte.SIZE - 1) > 0) ? 1 : 0) + 1;
        byte[] bytes = new byte[bytesLen];
        for (int i = 0, n = bytes.length; i < n; i++){
            bytes[i] = input.readByte();
        }
        boolVector = BitUtils.convertToBits(bytes);
        mem = new int[Stem.MEM_VECTOR_LENGTH];
        for (int i = 0; i < Stem.MEM_VECTOR_LENGTH; i++){
            mem[i] = input.readInt();
        }
    }
}
