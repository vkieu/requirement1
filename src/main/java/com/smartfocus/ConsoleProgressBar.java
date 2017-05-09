package com.smartfocus;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

@PropertySource(value = "file:global.properties")
@Component
public class ConsoleProgressBar {

    public static void main(String[] argv) throws Exception {
        //for DEMO
        String anim = "|/-\\";
        for (int x = 0; x <= 100; x++) {
            String data = "\r" + anim.charAt(x % anim.length()) + " " + x;
            System.out.write(data.getBytes());
            Thread.sleep(100);
        }
    }

    private String message = "";
    private long status = 0;

    @Value("${progress.bar.refresh.interval.ms:100}")
    private int refreshInterval;

    @Value("${progress.bar.enabled:true}")
    private boolean enabled;

    private long lastPrinted = 0;
    private int statusRoller = 0;
    private long started = 0;
    private final String DATE_FORMAT = "HH:mm:ss";

    public ConsoleProgressBar() {
        this.started = System.currentTimeMillis();
    }

    public void println(String message) {
        if(enabled) {
            System.out.println(message);
        }
    }

    public void setStatus(long status, String message) {
        this.status = status;
        this.message = message;
        printProgress();
    }

    private void printProgress() {
        if(!enabled) {
            return;
        }
        SimpleDateFormat df = new SimpleDateFormat(this.DATE_FORMAT);
        if (System.currentTimeMillis() - lastPrinted > refreshInterval) {
            String anim = "|/-\\";
            String data = "\r" + anim.charAt(statusRoller++ % anim.length())
                    + " elasped: " + (df.format(new Date(System.currentTimeMillis() - started)))
                    + " " + status + "..." + message;
            try {
                System.out.write(data.getBytes());
                lastPrinted = System.currentTimeMillis();
            } catch (IOException e) {
                //so what if we can't write to the console??? not possible
                System.out.println(e.getMessage());
            }
        }
    }
}
