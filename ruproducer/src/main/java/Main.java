import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class Main {
    public static void main(String[] args) throws IOException {
        new Producer("kafka1:9092").run();
        //Files.walk(Paths.get("")).filter(Files::isRegularFile).forEach(System.err::println);
    }
}
