package dev.frndpovoa.project1.databaseproxy;

import dev.frndpovoa.project1.databaseproxy.jdbc.Connection;

import java.util.Stack;

public class ConnectionHolder {
    private static final Stack<Connection> connections = new Stack<>();

    public static Connection getConnection() {
        return connections.isEmpty() ? null : connections.peek();
    }

    public static void pushConnection(final Connection connection) {
        connections.push(connection);
    }

    public static boolean popConnection(final Connection connection) {
        return connections.remove(connection);
    }
}
