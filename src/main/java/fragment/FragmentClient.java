package fragment;

import java.sql.*;
import java.util.*;
// Import Random for the "randomly selected fragment" requirement
import java.util.Random; 

public class FragmentClient {

    private Map<Integer, Connection> connectionPool;
    private Router router;
    private int numFragments;
    private Random random;

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

    /**
     * Task 4: Update a grade (route by studentId).
     */
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

    /**
     * Task 5: Delete a student's grade in a course.
     */
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

    /**
     * Task 6: Fetch student's name and email (deterministic route by studentId).
     * Returns "name,email" or null if not found.
     */
    public String getStudentProfile(String studentId) {
        if (studentId == null) return null;
        int fragmentId = router.getFragmentId(studentId);
        Connection conn = connectionPool.get(fragmentId);
        if (conn == null) {
            System.err.println("getStudentProfile: no connection for fragment " + fragmentId);
            return null;
        }
        String sql = "SELECT name, email FROM Student WHERE student_id = ?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, studentId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    String name = rs.getString("name");
                    String email = rs.getString("email");
                    return name + "," + email;
                }
                return null;
            }
        } catch (SQLException e) {
            System.err.println("getStudentProfile failed for id=" + studentId + " on frag=" + fragmentId + " -> " + e.getMessage());
            return null;
        }
    }

    /**
     * Task 7: Calculate the average score per department.
     * CRITICAL CHANGE: Executes on a RANDOMLY SELECTED fragment to simulate inconsistency.
     */
    public String getAvgScoreByDept() {
        // 1. Select ONE random fragment
        int randomFragmentId = random.nextInt(numFragments);
        Connection conn = connectionPool.get(randomFragmentId);
        
        if (conn == null) {
            System.err.println("getAvgScoreByDept: no connection for fragment " + randomFragmentId);
            return null;
        }

        // 2. Execute Query on that single fragment ONLY
        Map<String, long[]> agg = new HashMap<>();
        String sql = "SELECT c.department AS dept, g.score AS score " +
                     "FROM Grade g JOIN Course c ON g.course_id = c.course_id";
        
        try (PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                String dept = rs.getString("dept");
                int score = rs.getInt("score");
                if (dept == null) dept = "UNKNOWN";
                long[] cur = agg.computeIfAbsent(dept, k -> new long[2]); // [sum, count]
                cur[0] += score;
                cur[1] += 1;
            }
        } catch (SQLException e) {
            System.err.println("getAvgScoreByDept failed on fragment " + randomFragmentId + " -> " + e.getMessage());
            e.printStackTrace();
        }

        if (agg.isEmpty()) return null;

        List<String> lines = new ArrayList<>();
        for (Map.Entry<String, long[]> e : agg.entrySet()) {
            String dept = e.getKey();
            long sum = e.getValue()[0];
            long cnt = e.getValue()[1];
            double avg = cnt == 0 ? 0.0 : ((double) sum / cnt);
            lines.add(dept + ":" + String.format("%.2f", avg));
        }
        Collections.sort(lines);
        return String.join("\n", lines);
    }

    /**
     * Task 8: Find students who have taken the most courses.
     * CRITICAL CHANGE: Executes on a RANDOMLY SELECTED fragment to simulate inconsistency.
     */
    public String getAllStudentsWithMostCourses() {
        // 1. Select ONE random fragment
        int randomFragmentId = random.nextInt(numFragments);
        Connection conn = connectionPool.get(randomFragmentId);
        
        if (conn == null) {
            System.err.println("getAllStudentsWithMostCourses: no connection for fragment " + randomFragmentId);
            return null;
        }

        // 2. Execute Query on that single fragment ONLY
        Map<String, Integer> map = new HashMap<>();
        String sql = "SELECT student_id, COUNT(*) as cnt FROM Grade GROUP BY student_id";
        
        try (PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                map.put(rs.getString("student_id"), rs.getInt("cnt"));
            }
        } catch (SQLException e) {
            System.err.println("getAllStudentsWithMostCourses failed on fragment " + randomFragmentId + " -> " + e.getMessage());
            e.printStackTrace();
        }

        if (map.isEmpty()) return null;

        // Find max in this fragment
        int max = 0;
        for (int c : map.values()) if (c > max) max = c;

        List<String> best = new ArrayList<>();
        for (Map.Entry<String, Integer> e : map.entrySet()) {
            if (e.getValue() == max) best.add(e.getKey());
        }
        Collections.sort(best);

        return String.join(",", best) + ":" + max;
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