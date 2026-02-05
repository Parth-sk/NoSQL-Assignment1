//TO implement some functions for FragmentClient.java

package fragment;

import java.sql.*;
import java.util.*;

public class ParthClient {
    private Map<Integer, Connection> connectionPool;
    private Router router;
    private int numFragments;

    // --- ADDED CONFIGURATION (Required for Task 1) ---
    private static final String DB_USER = "postgres";
    // Using "1234" since you reset it earlier.
    private static final String DB_PASSWORD = "1234"; 
    private static final String BASE_URL = "jdbc:postgresql://localhost:5432/fragment";

    public ParthClient(int numFragments,Map<Integer, Connection> connectionPool) {
        
        this.numFragments = numFragments;
        this.router = new Router(numFragments);
        this.connectionPool = connectionPool;
    }

    public void updateGrade(String studentId, String courseId, int newScore) {
        if (studentId == null) return;
        int fragmentId = router.getFragmentId(studentId);
        Connection conn = connectionPool.get(fragmentId);
        if (conn == null) {
            System.err.println("updateGrade: no connection for fragment " + fragmentId);
            return;
        }
        String sql = "UPDATE Grade SET score = ? WHERE student_id = ? AND course_id = ?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, newScore);
            ps.setString(2, studentId);
            ps.setString(3, courseId);
            ps.executeUpdate();
        } catch (SQLException e) {
            System.err.println("updateGrade failed for sid=" + studentId + " on frag=" + fragmentId + " -> " + e.getMessage());
        }
    }

    public void deleteStudentFromCourse(String studentId, String courseId) {
        if (studentId == null) return;
        int fragmentId = router.getFragmentId(studentId);
        Connection conn = connectionPool.get(fragmentId);
        if (conn == null) {
            System.err.println("deleteStudentFromCourse: no connection for fragment " + fragmentId);
            return;
        }
        String sql = "DELETE FROM Grade WHERE student_id = ? AND course_id = ?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, studentId);
            ps.setString(2, courseId);
            ps.executeUpdate();
        } catch (SQLException e) {
            System.err.println("deleteStudentFromCourse failed for sid=" + studentId + " on frag=" + fragmentId + " -> " + e.getMessage());
        }
    }

    
}