import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class CreatePointDataset {

    public static void main(String[] args) {
        createPointDataset();
    }

    private static void createPointDataset(){
        try{
            FileWriter writer = new FileWriter("points");
            //Iterate to get 100 MB
            for (int i = 1; i <= 10800000; i++) {
                //Create each of the attributes
                String x = String.valueOf(generateRandomInteger(10000));
                String y = String.valueOf(generateRandomInteger(10000));

                String record = x + "," + y + "\n";
                writer.write(record);
            }
            writer.close();

        } catch (IOException e){
            System.out.println("An error occurred while writing to the file.");
            e.printStackTrace();
        }
    }

    private static int generateRandomInteger(int max) {
        //Ensures min is less than max
        if (1 > max) {
            System.out.println(1 + " " +  max);
            throw new IllegalArgumentException("Max must be greater than min");
        }
        Random random = new Random();
        return random.nextInt((max - 1) + 1) + 1;
    }

}
