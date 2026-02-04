package fragment;
import java.sql.*;
import java.util.*;

public class FragmentClient {
    private Map<Integer, Connection> connectionPool;
    private Router router;
    private int numFragments;
    private static final String DB_USER = "postgres"; 
    private static final String DB_PASSWORD = "1234"; 
    private static final String BASE_URL = "jdbc:postgresql://localhost:5432/fragment";

    public FragmentClient(int numFragments) {
        this.numFragments = numFragments;
        this.router = new Router(numFragments);
        this.connectionPool = new HashMap<>();
    }

    public void setupConnections() {
        try {
            Class.forName("org.postgresql.Driver");
            for (int i = 0; i < numFragments; i++) {
                String url = BASE_URL + i; 
                Connection conn = DriverManager.getConnection(url, DB_USER, DB_PASSWORD);
                connectionPool.put(i, conn);
                System.out.println("Connected to fragment: " + i);
            }
        } catch (Exception e) { e.printStackTrace(); }
    }
    // TASK 2: Route student to shard and Insert
    public void insertStudent(String studentId, String name, int age, String email) {
        try {
            // 1. Get the correct fragment ID from the Router
            int fragmentId = router.getFragmentId(studentId);
            
            // 2. Get the connection to that specific fragment
            Connection conn = connectionPool.get(fragmentId);

            // 3. Execute the standard SQL Insert
            String sql = "INSERT INTO Student (student_id, name, age, email) VALUES (?, ?, ?, ?)";
            PreparedStatement pstmt = conn.prepareStatement(sql);
            pstmt.setString(1, studentId);
            pstmt.setString(2, name);
            pstmt.setInt(3, age);
            pstmt.setString(4, email);
            pstmt.executeUpdate();
            pstmt.close();
            
            System.out.println("Student " + studentId + " inserted into Fragment " + fragmentId);

        } catch (SQLException e) {
            System.err.println("Error inserting student: " + e.getMessage());
        }
    }
    public void insertGrade(String s, String c, int sc) {}
    public void closeConnections() {
        try { for(Connection c : connectionPool.values()) if(c!=null) c.close(); } catch(Exception e){}
    }
}