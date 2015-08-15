package it.factbook.semantic;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import it.factbook.dictionary.Stem;
import it.factbook.util.BitUtils;

import java.io.Serializable;

public class SemanticVector implements Serializable, KryoSerializable {
    private int golem;
    private boolean[] boolVector;
    private int[] mem;

    public SemanticVector(int golem, boolean[] boolVector, int[] mem) {
        this.golem = golem;
        this.boolVector = boolVector;
        this.mem = mem;
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
    public void write(Kryo kryo, Output output) {
        output.writeByte(golem);
        for (byte each: BitUtils.convertToByteArray(boolVector)){
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
        int byteVectorSize = Stem.RI_VECTOR_LENGTH / Byte.SIZE;
        byte[] bytes = new byte[byteVectorSize];
        for (int i = 0; i < byteVectorSize; i++){
            bytes[i] = input.readByte();
        }
        boolVector = BitUtils.convertToBits(bytes);
        mem = new int[Stem.MEM_VECTOR_LENGTH];
        for (int i = 0; i < Stem.MEM_VECTOR_LENGTH; i++){
            mem[i] = input.readInt();
        }
    }
}
