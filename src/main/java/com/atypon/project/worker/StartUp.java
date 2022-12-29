package com.atypon.project.worker;

import java.io.IOException;
import java.util.stream.Collectors;
import java.util.List;
import java.util.stream.Stream;

public class StartUp {

    public static void main(String[] args) throws Exception {
        int numNodes = Integer.parseInt(args[0]);

        // build the image from the docker file
//        exec("docker build -t worker .");
        exec("docker",
                "run",
                "-d",
                "-p",
                "8000:8080",
                "--name", "bootstrap",
                "--network", "cluster",
                "--ip", "10.1.4.0",
                "--env", "NODE_ID=bootstrap",
                "--env","NUM_NODES="+Integer.toString(numNodes),
                "--env",
                "BOOTSTRAP=yes",
                "worker");
        Thread.sleep(1000 * 30);

       for(int i = 1; i <= numNodes; i++) {
            exec("docker",
                    "run",
                    "-d",
                    "-p", 8000 + i + ":8080",
                    "--name", "worker_" + i,
                    "--network", "cluster",
                    "--ip", "10.1.4." + i,
                    "--env", "NODE_ID=node_" + i,
                    "worker");
        }
    }

    static void exec(String... args) throws IOException {
        boolean isWindows = System.getProperty("os.name").toLowerCase().startsWith("windows");
        List<String> commands;
        if (isWindows)
            commands = Stream.of("cmd.exe", "/c").collect(Collectors.toList());
        else
            commands = Stream.of("sh", "-c").collect(Collectors.toList());
        for (String cmd : args)
            commands.add(cmd);
        ProcessBuilder builder = new ProcessBuilder(commands);
        builder.start();
    }

}
