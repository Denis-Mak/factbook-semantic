package it.factbook.semantic;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestSemanticVector {
    private static SemanticVector semanticVector =
            new SemanticVector(1, "EA0TFiY3OEBCRkxaYGFpeXo=", "[57,3654,648,3764,477,847,458,593]");

    @Test
    public void testSerialization(){
        Kryo kryo = new Kryo();
        Output output = new Output(1024);
        kryo.writeObject(output, semanticVector);
        Input input = new Input(output.toBytes());
        SemanticVector deserializedVector = kryo.readObject(input, SemanticVector.class);
        assertEquals(semanticVector, deserializedVector);
    }
}
