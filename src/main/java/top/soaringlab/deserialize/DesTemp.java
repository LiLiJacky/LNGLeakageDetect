package top.soaringlab.deserialize;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;

public class DesTemp {
    public static Object deserializeToObject(String s) throws Exception{
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(s.getBytes("ISO-8859-1"));
        ObjectInputStream objIn = new ObjectInputStream(byteArrayInputStream);
        Object obj =objIn.readObject();
        return obj;
    }
}
