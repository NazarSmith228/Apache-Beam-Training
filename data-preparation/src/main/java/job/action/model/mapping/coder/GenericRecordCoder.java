package job.action.model.mapping.coder;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ConcurrentHashMap;

public class GenericRecordCoder extends AtomicCoder<GenericRecord> {

    public static GenericRecordCoder of() {
        return new GenericRecordCoder();
    }

    private static final ConcurrentHashMap<String, AvroCoder<GenericRecord>> avroCoders = new ConcurrentHashMap<>();

    @Override
    public void encode(GenericRecord value, OutputStream outStream) throws IOException {
        String schemaString = value.getSchema().toString();
        String schemaHash = getHash(schemaString);
        StringUtf8Coder.of().encode(schemaString, outStream);
        StringUtf8Coder.of().encode(schemaHash, outStream);
        AvroCoder<GenericRecord> coder = avroCoders.computeIfAbsent(schemaHash,
                s -> AvroCoder.of(value.getSchema()));
        coder.encode(value, outStream);
    }

    @Override
    public GenericRecord decode(InputStream inStream) throws IOException {
        String schemaString = StringUtf8Coder.of().decode(inStream);
        String schemaHash = StringUtf8Coder.of().decode(inStream);
        AvroCoder<GenericRecord> coder = avroCoders.computeIfAbsent(schemaHash,
                s -> AvroCoder.of(new Schema.Parser().parse(schemaString)));
        return coder.decode(inStream);
    }

    private String getHash(String value) {
        try {
            MessageDigest messageDigest = MessageDigest.getInstance("SHA-256");
            messageDigest.update(value.getBytes());
            return new String(messageDigest.digest());
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalArgumentException("Invalid hash algorithm");
        }
    }
}
