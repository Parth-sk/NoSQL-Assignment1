package fragment;

import java.sql.*;
import java.util.*;

public class AgamClient {

    private Map<Integer, Connection> connectionPool;
    private Router router;
    private int numFragments;

    public AgamClient(int numFragments, Map<Integer, Connection> connectionPool) {
        this.numFragments = numFragments;
        this.router = new Router(numFragments);
        this.connectionPool = connectionPool;
    }

    /**
     * Task 2: Route the student to the correct shard and execute the INSERT.
     */
    public void insertStudent(String studentId, String name, int age, String email) {
        if (studentId == null) return;
        int fragmentId = router.getFragmentId(studentId);
        Connection conn = connectionPool.get(fragmentId);
        if (conn == null) {
            System.err.println("insertStudent: no connection for fragment " + fragmentId);
            return;
        }
        String sql = "INSERT INTO Student (student_id, name, age, email) VALUES (?, ?, ?, ?)";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, studentId);
            ps.setString(2, name);
            ps.setInt(3, age);
            ps.setString(4, email);
            ps.executeUpdate();
        } catch (SQLException e) {
            System.err.println("insertStudent failed for id=" + studentId + " on frag=" + fragmentId + " -> " + e.getMessage());
            // Duplicate key errors are expected in some workloads, so we just print them.
        }
    }

    /**
     * Task 3: Route the grade to the correct shard and execute the INSERT.
     */
    public void insertGrade(String studentId, String courseId, int score) {
        if (studentId == null) return;
        int fragmentId = router.getFragmentId(studentId);
        Connection conn = connectionPool.get(fragmentId);
        if (conn == null) {
            System.err.println("insertGrade: no connection for fragment " + fragmentId);
            return;
        }
        String sql = "INSERT INTO Grade (student_id, course_id, score) VALUES (?, ?, ?)";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, studentId);
            ps.setString(2, courseId);
            ps.setInt(3, score);
            ps.executeUpdate();
        } catch (SQLException e) {
            System.err.println("insertGrade failed for sid=" + studentId + " on frag=" + fragmentId + " -> " + e.getMessage());
        }
    }
}