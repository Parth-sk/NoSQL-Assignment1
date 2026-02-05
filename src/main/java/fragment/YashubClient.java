package fragment;

import java.sql.*;
import java.util.*;

public class YashubClient {

    private Map<Integer, Connection> connectionPool;
    private Router router;
    private int numFragments;
    private Random random;

    public YashubClient(int numFragments, Map<Integer, Connection> connectionPool) {
        this.numFragments = numFragments;
        this.connectionPool = connectionPool;
        this.router = new Router(numFragments);
        this.random = new Random();
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
        return String.join(";", lines);
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