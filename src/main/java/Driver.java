import fragment.FragmentClient;
import java.io.*;
import java.util.Scanner;

public class Driver {
    public static void main(String[] args) {
        // You can change this to 1 for the "Control" run, and 3 for the "Actual" run
        int numFragments = 3; 
        
        FragmentClient client = new FragmentClient(numFragments);
        try {
            // 1. Connect to databases
            client.setupConnections();

            // 2. WIPE DATABASE CLEAN (New line added here)
            // This prevents "Duplicate Key" errors by starting fresh every time.
            client.cleanDatabase(); 
            
            // 3. Read the workload file
            InputStream inputStream = Driver.class.getClassLoader().getResourceAsStream("workload.txt");
            
            if (inputStream == null) {
                System.out.println("ERROR: workload.txt not found!");
                return;
            }

            Scanner scanner = new Scanner(inputStream);
            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                if (line.trim().isEmpty()) continue; // Skip empty lines

                String[] parts = line.split(",");
                String command = parts[0].trim();

                if (command.equals("INSERT_STUDENT")) {
                    // Format: INSERT_STUDENT, ID, Name, Age, Email
                    client.insertStudent(parts[1].trim(), parts[2].trim(), Integer.parseInt(parts[3].trim()), parts[4].trim());
                    System.out.println("Processed: " + line);
                } 
                else if (command.equals("INSERT_GRADE")) {
                    // Format: INSERT_GRADE, StudentID, CourseID, Score
                    client.insertGrade(parts[1].trim(), parts[2].trim(), Integer.parseInt(parts[3].trim()));
                    System.out.println("Processed: " + line);
                }
            }
            scanner.close();
            
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            client.closeConnections();
        }
    }
}