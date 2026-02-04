package fragment;

import java.sql.*;
import java.util.*;

public class FragmentClient {

    private Map<Integer, Connection> connectionPool;
    private Router router;
    private int numFragments;

    // --- ADDED CONFIGURATION (Required for Task 1) ---
    private static final String DB_USER = "postgres";
    // Using "1234" since you reset it earlier.
    private static final String DB_PASSWORD = "1234"; 
    private static final String BASE_URL = "jdbc:postgresql://localhost:5432/fragment";

    public FragmentClient(int numFragments) {
        this.numFragments = numFragments;
        this.router = new Router(numFragments);
        this.connectionPool = new HashMap<>();
    }

    /**
     * Task 1: Initialize JDBC connections to all N Fragments.
     */
    public void setupConnections() {
        try {
            // Load the PostgreSQL driver
            Class.forName("org.postgresql.Driver");
            
            for (int i = 0; i < numFragments; i++) {
                // Construct the URL: e.g., jdbc:postgresql://localhost:5432/fragment0
                String url = BASE_URL + i;
                
                // Create the connection
                Connection conn = DriverManager.getConnection(url, DB_USER, DB_PASSWORD);
                
                // Store it in the pool
                connectionPool.put(i, conn);
                System.out.println("Connected to fragment: " + i);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Task 2: Route the student to the correct shard and execute the INSERT.
     */
    public void insertStudent(String studentId, String name, int age, String email) {
        try {
            // 1. Get the correct fragment ID from the Router
            int fragmentId = router.getFragmentId(studentId);
            
            // 2. Get the connection to that specific fragment
            Connection conn = connectionPool.get(fragmentId);

            // 3. Execute the SQL Insert
            if (conn != null) {
                String sql = "INSERT INTO Student (student_id, name, age, email) VALUES (?, ?, ?, ?)";
                PreparedStatement pstmt = conn.prepareStatement(sql);
                pstmt.setString(1, studentId);
                pstmt.setString(2, name);
                pstmt.setInt(3, age);
                pstmt.setString(4, email);
                pstmt.executeUpdate();
                pstmt.close();
                // System.out.println("Inserted Student " + studentId + " into fragment " + fragmentId);
            }
        } catch (Exception e) {
            System.err.println("Error inserting student: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Task 3: Route the grade to the correct shard and execute the INSERT.
     */
    public void insertGrade(String studentId, String courseId, int score) {
        try {
            // 1. Route based on studentId (Co-location strategy)
            int fragmentId = router.getFragmentId(studentId);
            
            // 2. Get the connection
            Connection conn = connectionPool.get(fragmentId);

            // 3. Execute the SQL Insert
            if (conn != null) {
                String sql = "INSERT INTO Grade (student_id, course_id, score) VALUES (?, ?, ?)";
                PreparedStatement pstmt = conn.prepareStatement(sql);
                pstmt.setString(1, studentId);
                pstmt.setString(2, courseId);
                pstmt.setInt(3, score);
                pstmt.executeUpdate();
                pstmt.close();
                // System.out.println("Inserted Grade for " + studentId + " into fragment " + fragmentId);
            }
        } catch (Exception e) {
            System.err.println("Error inserting grade: " + e.getMessage());
            e.printStackTrace();
        }
    }

    // --- Placeholders for Future Tasks (Do not change) ---
    public void updateGrade(String studentId, String courseId, int newScore) {
        try {
            // Your code here:
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void deleteStudentFromCourse(String studentId, String courseId) {
        try {
            // Your code here:
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public String getStudentProfile(String studentId) {
        try {
            // Your code here
            return null; 
        } catch (Exception e) {
            e.printStackTrace();
            return "ERROR";
        }
    }

    public String getAvgScoreByDept() {
        try {
            // Your code here
            return null;
        } catch (Exception e) {
            e.printStackTrace();
            return "ERROR";
        }
    }

    public String getAllStudentsWithMostCourses() {
        try {
            // Your code here
            return null;
        } catch (Exception e) {
            e.printStackTrace();
            return "ERROR";
        }
    }

    public void closeConnections() {
        try {
            if (connectionPool != null) {
                for (Connection c : connectionPool.values()) {
                    if (c != null && !c.isClosed()) {
                        c.close();
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}