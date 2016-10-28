package org.apache.nifi.processors.gettcp;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.Socket;

/**
 * Created by ddewaele on 28/10/16.
 */
public class TestClient {

    private static final int EOT = 0x04;

    public static void main(String[] args) throws Exception {

        Socket clientSocket = new Socket("localhost", 5005);
        InputStream is = clientSocket.getInputStream();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        while(true) {

            int n = is.read();
            if( n < 0 ) break;

            if ( n == EOT) {
                String msg = new String(baos.toByteArray());
                System.out.println("read = " + msg);
                baos = new ByteArrayOutputStream();
            } else {
               baos.write(n);
            }

        }

        //byte data[] = baos.toByteArray();

    }
}
