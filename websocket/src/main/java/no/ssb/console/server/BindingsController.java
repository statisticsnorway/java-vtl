package kohl.hadrien.console.server;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Controller;

import java.text.SimpleDateFormat;
import java.util.Date;

@Controller
public class BindingsController {

    @Autowired
    private UserBindings userBindings;

    @MessageMapping("/chat")
    @SendTo("/topic/messages")
    public String send(String message) throws Exception {
        String time = new SimpleDateFormat("HH:mm").format(new Date());
        Thread.sleep(1000); // simulated delay
        return time + " -> " + message;
    }
}
