package it.factbook.semantic;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;

public class SemanticSearchTestClient {
    private static final String PROFILE = "{" +
            "  \"id\": 64," +
            "  \"name\": \"Test profile\"," +
            "  \"userId\": 1," +
            "  \"kept\": 3," +
            "  \"deleted\": 0," +
            "  \"found\": 0," +
            "  \"created\": 1410441938000," +
            "  \"lastVisited\": 1410441938000," +
            "  \"updated\": 1410441938000," +
            "  \"lastDocFound\": 1410441938000," +
            "  \"query\": \"\"," +
            "  \"lines\": [" +
            "    {" +
            "      \"id\": 25853," +
            "      \"mem\": [175, 686, 2112, 3347, 1537, 2994, 885, 653]," +
            "      \"randomIndex\": \"IwIHCBITFRobHB4jJygpKzAxMjQ2OTo7QUhLUVVgYmlrd3p8\"," +
            "      \"weight\": 15," +
            "      \"words\": [" +
            "        [" +
            "          {" +
            "            \"id\": 167398," +
            "            \"word\": \"общемировой\"," +
            "            \"lang\": \"UNKNOWN\"," +
            "            \"golem\": \"WIKI_RU\"," +
            "            \"ending\": 313," +
            "            \"tag\": 17," +
            "            \"root\": \"(общемиров\"," +
            "            \"stemIdf\": 12," +
            "            \"stemId\": 12206," +
            "            \"semId\": 207" +
            "          }," +
            "          {" +
            "            \"id\": 3307080," +
            "            \"word\": \"репертуар\"," +
            "            \"lang\": \"UNKNOWN\"," +
            "            \"golem\": \"WIKI_RU\"," +
            "            \"ending\": 445," +
            "            \"tag\": 17," +
            "            \"root\": \"(репертуар\"," +
            "            \"stemIdf\": 15," +
            "            \"stemId\": 6316," +
            "            \"semId\": 218" +
            "          }" +
            "        ]]" +
            "    }" +
            "  ]" +
            "}";
    public static void main(String[] args) throws IOException {
        try (
                Socket socket = new Socket("localhost", 3434);
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                BufferedReader in = new BufferedReader(
                        new InputStreamReader(socket.getInputStream()));
        ) {
            String fromServer;

            out.println(PROFILE);

            while ((fromServer = in.readLine()) != null) {
                System.out.println("Server: " + fromServer);
            }
        } catch (UnknownHostException e) {
            System.err.println("Don't know about host ");
            System.exit(1);
        } catch (IOException e) {
            System.err.println("Couldn't get I/O for the connection to ");
            System.exit(1);
        }
    }
}
