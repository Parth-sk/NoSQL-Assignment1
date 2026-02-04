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

    // Empty placeholders for now
    public void insertStudent(String s, String n, int a, String e) {}
    public void insertGrade(String s, String c, int sc) {}
    public void closeConnections() {
        try { for(Connection c : connectionPool.values()) if(c!=null) c.close(); } catch(Exception e){}
    }
}