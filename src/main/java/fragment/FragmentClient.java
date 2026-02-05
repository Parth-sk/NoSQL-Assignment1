package fragment;

import java.sql.*;
// Import Random for the "randomly selected fragment" requirement
import java.util.*;
public class FragmentClient {

    private Map<Integer, Connection> connectionPool;
    private Router router;
    private int numFragments;
    private Random random;
    private ParthClient parthClient;
    private AgamClient agamClient;
    private YashubClient yashubClient;

    // DB configuration - change only if your DB credentials differ
    private static final String DB_USER = "postgres";
    private static final String DB_PASSWORD = "1234";
    // Ensure this matches your local setup (e.g. fragment0, fragment1, fragment2)
    private static final String BASE_URL = "jdbc:postgresql://localhost:5432/fragment"; 

    public FragmentClient(int numFragments) {
        this.numFragments = Math.max(1, numFragments);
        this.router = new Router(this.numFragments);
        this.connectionPool = new HashMap<>();
        this.random = new Random();
        this.parthClient = new ParthClient(numFragments, connectionPool);
        this.agamClient = new AgamClient(numFragments, connectionPool);
        this.yashubClient = new YashubClient(numFragments, connectionPool);
    }

    /**
     * Initialize JDBC connections to all N Fragments.
     */
    public void setupConnections() {
        try {
            Class.forName("org.postgresql.Driver");
            for (int i = 0; i < numFragments; i++) {
                // Connect to databases named fragment0, fragment1, etc.
                String url = BASE_URL + i; 
                Connection conn = DriverManager.getConnection(url, DB_USER, DB_PASSWORD);
                conn.setAutoCommit(true);
                connectionPool.put(i, conn);
                System.out.println("Connected to fragment " + i + " (" + url + ")");
            }
        } catch (Exception e) {
            System.err.println("setupConnections failed: " + e.getMessage());
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    /**
     * Task 2: Route the student to the correct shard and execute the INSERT.
     */
    public void insertStudent(String studentId, String name, int age, String email) {
        agamClient.insertStudent(studentId, name, age, email);
    }

    /**
     * Task 3: Route the grade to the correct shard and execute the INSERT.
     */
    public void insertGrade(String studentId, String courseId, int score) {
        agamClient.insertGrade(studentId, courseId, score);
    }

    /**
     * Task 4: Update a grade (route by studentId).
     */
    public void updateGrade(String studentId, String courseId, int newScore) {
        parthClient.updateGrade(studentId, courseId, newScore);
    }

    /**
     * Task 5: Delete a student's grade in a course.
     */
    public void deleteStudentFromCourse(String studentId, String courseId) {
        parthClient.deleteStudentFromCourse(studentId, courseId);
    }

    /**
     * Task 6: Fetch student's name and email (deterministic route by studentId).
     * Returns "name,email" or null if not found.
     */
    public String getStudentProfile(String studentId) {
        return yashubClient.getStudentProfile(studentId);
    }

    /**
     * Task 7: Calculate the average score per department.
     * CRITICAL CHANGE: Executes on a RANDOMLY SELECTED fragment to simulate inconsistency.
     */
    public String getAvgScoreByDept() {
        return yashubClient.getAvgScoreByDept();
    }

    /**
     * Task 8: Find students who have taken the most courses.
     * CRITICAL CHANGE: Executes on a RANDOMLY SELECTED fragment to simulate inconsistency.
     */
    public String getAllStudentsWithMostCourses() {
        return yashubClient.getAllStudentsWithMostCourses();
    }

    /**
     * Clean close connections.
     */
    public void closeConnections() {
        for (Connection c : connectionPool.values()) {
            if (c == null) continue;
            try {
                if (!c.isClosed()) c.close();
            } catch (SQLException ignored) {}
        }
    }
}