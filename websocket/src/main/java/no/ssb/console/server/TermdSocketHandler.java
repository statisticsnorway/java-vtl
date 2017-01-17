package kohl.hadrien.console.server;

import io.termd.core.http.HttpTtyConnection;
import io.termd.core.tty.TtyConnection;
import kohl.hadrien.console.tty.VTLConsole;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class TermdSocketHandler extends TextWebSocketHandler {

    @Autowired
    private ThreadPoolTaskExecutor ptsExecutors;

    private HttpTtyConnection connection;

    @Override
    public void afterConnectionEstablished(WebSocketSession session) throws Exception {
        Consumer<TtyConnection> handler = new VTLConsole();
        connection = new HttpTtyConnection() {
            @Override
            protected void write(byte[] buffer) {
                try {
                    session.sendMessage(
                            new TextMessage(buffer)
                    );
                } catch (IOException e) {
                    close();
                }
            }

            @Override
            public void close() {
                try {
                    session.close();
                } catch (IOException e) {
                    // Ignore
                }
            }

            @Override
            public void execute(Runnable task) {
                ptsExecutors.execute(task);
            }

            @Override
            public void schedule(Runnable task, long delay, TimeUnit unit) {
                ptsExecutors.execute(task, unit.toMillis(delay));
            }
        };
        handler.accept(connection);
    }

    @Override
    protected void handleTextMessage(WebSocketSession session, TextMessage message) throws Exception {
        connection.writeToDecoder(message.getPayload());
    }
}
