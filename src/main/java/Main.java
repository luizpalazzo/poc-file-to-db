import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.annotations.QuarkusMain;
import service.Extraction;

@QuarkusMain
public class Main {

    private final static String SQL = "SELECT * FROM table_test WHERE id <= 4";


    public static void main(String[] args){

        System.out.println("Started");
        Extraction extractionService = new Extraction();
        extractionService.start(SQL);
        Quarkus.run(args);


    }
}
