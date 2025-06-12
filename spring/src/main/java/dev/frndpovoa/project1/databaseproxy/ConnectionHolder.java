package dev.frndpovoa.project1.databaseproxy;

import dev.frndpovoa.project1.databaseproxy.jdbc.Connection;
import dev.frndpovoa.project1.databaseproxy.proto.DatabaseProxyGrpc;
import lombok.Builder;

import java.util.Stack;

@Builder
public class ConnectionHolder {
    private static ThreadLocal<ConnectionHolder> defaultInstance = new InheritableThreadLocal<>();

    public static ConnectionHolder getDefaultInstance() {
        return defaultInstance.get();
    }

    @Builder.Default
    private final Stack<Connection> connections = new Stack<>();
    private DatabaseProxyGrpc.DatabaseProxyBlockingStub blockingStub;

    public Connection getConnection() {
        return connections.isEmpty() ? null : connections.peek();
    }

    public void pushConnection(final Connection connection) {
        connections.push(connection);
    }

    public void popConnection() {
        connections.pop();
    }

    public static ConnectionHolder.ConnectionHolderBuilder builder() {
        return new CustomConnectionHolderBuilder();
    }

    private static class CustomConnectionHolderBuilder extends ConnectionHolder.ConnectionHolderBuilder {
        @Override
        public ConnectionHolder build() {
            final ConnectionHolder thiz = super.build();
            defaultInstance.set(thiz);
            return thiz;
        }
    }
}
