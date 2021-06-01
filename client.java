import java.net.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

import org.w3c.dom.*;

import java.io.*;

public class Client {
    // initialize socket and input output streams
    private static Socket socket = null;
    private BufferedReader input = null;
    private DataOutputStream out = null;
    private BufferedReader in = null;

    // constructor to put ip address and port
    public Client(String address, int port) throws IOException {
        connect(address, port);
        input = new BufferedReader(new InputStreamReader(System.in));
        out = new DataOutputStream(socket.getOutputStream());
        in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
    }

    private void start() {
        sendMessage("HELO");
        readMessage();

        // Send user details
        sendMessage("AUTH " + System.getProperty("user.name"));
        readMessage();

        // handshake completed
        boolean connected = true;

        //  arrayList of server objects from:
        ArrayList<Server> servers = new ArrayList<Server>();

        // arrayList for jobs
        ArrayList<Job> jobs = new ArrayList<Job>();

        // Tells client it is ready to recieve commands
        sendMessage("REDY");

        // temp string to hold readMessage data
        // we check the contents of this string, rather than call readMessage()
        String msg = readMessage();

        // SCHEDULES JOB TO LARGEST SERVER
        while (connected) {
            // Job completed we tell ds-server we are ready
            if (msg.contains("JCPL")) {
                sendMessage("REDY");
                msg = readMessage();
                // there are no more jobs left
            } else if (msg.contains("NONE")) { // there are no more jobs left
                connected = false;
                sendMessage("QUIT");
            } else {

                // Get next message
                if (msg.contains("OK")) {
                    sendMessage("REDY");
                    msg = readMessage();
                }

                // we have a JOB incoming, so we create a job objet based on it
                if (msg.contains("JOBN")) {
                    jobs.add(plusJob(msg)); // create job

                    // the job arrayList will only ever have 1 item in it at a time...
                    sendMessage(getCapable(jobs.get(0))); // GETS Capable called
                    msg = readMessage();

                    sendMessage("OK");

                    // list of capable servers are added to arrayList of server objects
                    msg = readMessage();
                    servers = plusServer(msg);
                    sendMessage("OK");

                    // we should receive a "." here
                    msg = readMessage();

                    sendMessage(myAlgo (servers, jobs)); // Scheduling algorithm called here
                    msg = readMessage();

                    // only need one job at a time
                    jobs.remove(0);
                }
            }
        }

        // close the connection
        try {

            // QUIT hand-shake, must receive confirmation from server for quit
            if (readMessage().contains("QUIT")) {
                input.close();
                out.close();
                socket.close();
            }

        } catch (IOException i) {
            // System.out.println(i);
        }

        // Exit the program
        System.exit(1);
    }

    // Send message to server
    private void sendMessage(String outStr) {
        byte[] byteMsg = outStr.getBytes();
        try {
            out.write(byteMsg);
        } catch (IOException e) {
         //   e.printStackTrace();
        }

        // Display output from client
    //    System.out.println("OUT: " + outStr);
    }

    // Read message from server
    private String readMessage() {
        String inStr = "";
        char[] cbuf = new char[Short.MAX_VALUE * 2];
        try {
            in.read(cbuf);
        } catch (IOException e) {
        //    e.printStackTrace();
        }
        inStr = new String(cbuf, 0, cbuf.length);

        // Display input from server
    //    System.out.println("INC: " + inStr);

        return inStr;
    }

    public ArrayList<Server> plusServer(String s) {
        s = s.trim();

        ArrayList<Server> List = new ArrayList<Server>();

        String[] lines = s.split("\\r?\\n");

        for (String line : lines) {
            String[] splitStr = line.split("\\s+");

            Server server = new Server(splitStr[0], Integer.parseInt(splitStr[1]), splitStr[2],
                    Integer.parseInt(splitStr[3]), Integer.parseInt(splitStr[4]), Integer.parseInt(splitStr[5]),
                    Integer.parseInt(splitStr[6]), Integer.parseInt(splitStr[7]), Integer.parseInt(splitStr[8]));
            List.add(server);
        }

        return List;
    }

    //
    // create a new job 
    //
    public Job plusJob(String job) {
        job = job.trim();
        String[] splitStr = job.split("\\s+");

        Job j = new Job(Integer.parseInt(splitStr[1]), Integer.parseInt(splitStr[2]), Integer.parseInt(splitStr[3]),
                Integer.parseInt(splitStr[4]), Integer.parseInt(splitStr[5]), Integer.parseInt(splitStr[6]));

        // returns job object to  arrayList
        return j;
    }

    //Function for finding capable server
    public String getCapable(Job j) {
        return ("GETS Capable " + j.getCoreReq() + " " + j.getMemoryReq() + " " + j.getDiskReq());
    }

    public String myAlgo (ArrayList<Server> servers, ArrayList<Job> job){
		//  information string
		String Info = "";

		for (Server server: servers) {
            if (
            
            server.getMemory() >= job.get(0).getMemoryReq() &&
            job.get(0).getStartTime() >= job.get(0).getRunTime()) {
					Info = server.getType() + " " + server.getID();
					return "SCHD " + job.get(0).getID() + " " + Info;
			}
			else {
				// Send job to first server
				Info = servers.get(0).getType() + " " + servers.get(0).getID();
			}
		}
		return "SCHD " + job.get(0).getID() + " " + Info;
	}


    // Establishes connection to initiate handhsake
    private static void connect(String address, int port) {
        double secondsToWait = 1;
        int tryNum = 1;
        while (true) {
            try {
            //    System.out.println("Connecting to server at: " + address + ":" + port);
                socket = new Socket(address, port);
            //    System.out.println("Connected");
                break;
            } catch (IOException e) {
                secondsToWait = Math.min(30, Math.pow(2, tryNum));
                tryNum++;
            //    System.out.println("Connection timed out, retrying in  " + (int) secondsToWait + " seconds ...");
                try {
                    TimeUnit.SECONDS.sleep((long) secondsToWait);
                } catch (InterruptedException ie) {
                    ie.printStackTrace();
                }
            }
        }
    }

    public static void main(String args[]) throws IOException {
        Client client = new Client("127.0.0.1", 50000);
        client.start();
    }

}