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

    public ParthClient(int numFragments) {
        this.numFragments = numFragments;
        this.router = new Router(numFragments);
        this.connectionPool = new HashMap<>();
    }

}