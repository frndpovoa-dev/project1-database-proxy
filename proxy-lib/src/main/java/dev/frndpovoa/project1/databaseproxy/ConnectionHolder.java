package dev.frndpovoa.project1.databaseproxy;

/*-
 * #%L
 * database-proxy-lib
 * %%
 * Copyright (C) 2025 Fernando Lemes Povoa
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import dev.frndpovoa.project1.databaseproxy.jdbc.Connection;

import java.util.Stack;

public class ConnectionHolder {
    private static final ThreadLocal<Stack<Connection>> connections = ThreadLocal.withInitial(Stack::new);

    public static Connection getConnection() {
        return connections.get().isEmpty() ? null : connections.get().peek();
    }

    public static void pushConnection(final Connection connection) {
        connections.get().push(connection);
    }

    public static boolean popConnection(final Connection connection) {
        return connections.get().remove(connection);
    }
}
