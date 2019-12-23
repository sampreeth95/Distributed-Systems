package edu.buffalo.cse.cse486586.groupmessenger2;
import android.app.Activity;
import android.content.Context;
import android.os.AsyncTask;
import android.os.Bundle;
import android.telephony.TelephonyManager;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.TextView;
import java.util.Collections;
import java.util.*;

import java.io.BufferedReader;
import java.util.concurrent.TimeUnit;

import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Array;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.io.PrintWriter;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.net.Uri;

import org.apache.http.ConnectionClosedException;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Dictionary;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * GroupMessengerActivity is the main Activity for the assignment.
 *
 * @author stevko
 *
 *
 */
/*USED THE SAME CODE AS GRP MESSENGER 1 JUST ADDED isis LOGIC*/
public class GroupMessengerActivity extends Activity {
    static final String TAG = GroupMessengerActivity.class.getSimpleName();
    static final String[] REMOTE_PORT = {"11108", "11112", "11116", "11120", "11124"};
    static final int SERVER_PORT = 10000;
    private static final String KEY_FIELD_DB = "key";
    private static final String VALUE_FIELD_DB = "value";
    int primary_Key;
    public static int id = 0;
    public static AtomicInteger ss_Id =new AtomicInteger(-1);
    public static AtomicInteger sa_Id =new AtomicInteger(-1);
    public static String failed_Port ;
    ContentValues values = new ContentValues();
    private Uri mUri;
    ContentResolver contentResolver;
    HashMap<String, Integer> d = new HashMap<String, Integer>();
    int i = 0;
    public static int c = 0;
    public static int c1 = 0;

    class message {
        String msg_Id = "";
        String msg = "";
        int sa_Id = 0;
        String port_Number = "";
        int d=0;

        public message(String msg_Id, String msg, int sa_Id, String port_Number,int d) {
            this.msg_Id = msg_Id;
            this.msg = msg;
            this.sa_Id = sa_Id;
            this.port_Number = port_Number;
            this.d=d;
        }
        /*ref:https://stackoverflow.com/questions/8520808/how-to-remove-specific-object-from-arraylist-in-java(FOR DETECTING SAME MESSAGE OBJECT)*/
        public boolean equals(Object obj) {

            message msg = (message) obj;
            if (this.msg_Id.equals(msg.msg_Id))
                return true;
            else
                return false;

        }
    }

    PriorityQueue<message> pQueue =
            new PriorityQueue<message>(20, new messageComparator());
    PriorityQueue<message> pQueue1 =
            new PriorityQueue<message>(20, new messageComparator());


    class messageComparator implements Comparator<message> {

        // Overriding compare()method of Comparator
        // for descending order of cgpa
        public int compare(message m1, message m2) {
            if (m1.sa_Id < m2.sa_Id) {
                return -1;
            } else if (m1.sa_Id > m2.sa_Id) {
                return 1;
            }
            else{
                Log.i(TAG, "got same agreed number for 2 messages compare port number");
                if(Integer.parseInt(m1.port_Number)<Integer.parseInt(m2.port_Number)){
                    return -1;

                }
                else if(Integer.parseInt(m1.port_Number)>Integer.parseInt(m2.port_Number)){
                    return 1;
                }
                else{
                    Log.i(TAG, "looking for message id");
                    if(m1.msg_Id.equals(m2.msg_Id)){
                        return -1;
                    }
                    else if(!m1.msg_Id.equals(m2.msg_Id))
                        return 1;
                }
            }
            return 0;

        }
    }
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority("edu.buffalo.cse.cse486586.groupmessenger2.provider");
        uriBuilder.scheme("content");

        mUri = uriBuilder.build();
        contentResolver = getContentResolver();
        primary_Key = 0;
        setContentView(R.layout.activity_group_messenger);

        TelephonyManager
                tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        try {


            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Can't initiate ServerSocket");
            return;
        }


        /*
         * TODO: Use the TextView to display your messages. Though there is no grading component
         * on how you display the messages, if you implement it, it'll make your debugging easier.
         */
        TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());
        final EditText editText = (EditText) findViewById(R.id.editText1);





        /*
         * Registers OnPTestClickListener for "button1" in the layout, which is the "PTest" button.
         * OnPTestClickListener demonstrates how to access a ContentProvider.
         */
        findViewById(R.id.button1).setOnClickListener(
                new OnPTestClickListener(tv, getContentResolver()));
        final Button send = (Button) findViewById(R.id.button4);
        send.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                Log.i(TAG, "From EditText1(given by user) : " + editText.getText().toString());
                String msg = editText.getText().toString();
                editText.setText("");
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, myPort);

            }

            /*
             * TODO: You need to register and implement an OnClickListener for the "Send" button.
             * In your implementation you need to get the message from the input box (EditText)
             * and send it to other AVDs.
             */
        });
    }


    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_group_messenger, menu);
        for (String remote_port : REMOTE_PORT) {
            d.put(remote_port, 0);
        }

        return true;
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            try {
                ServerSocket serverSocket = sockets[0];
                Socket mysocket = null;
                String msgfromclient;
                String acknowledgment;
                while (true) {
                    mysocket = serverSocket.accept();
                    Log.d(TAG, "sever got s message" );
                    PrintWriter output = new PrintWriter(mysocket.getOutputStream(), true);
                    BufferedReader input = new BufferedReader(new InputStreamReader(mysocket.getInputStream()));
                    msgfromclient = input.readLine();
                    if(msgfromclient==null){
                        continue;
                    }
                    String[] msgfromclient_parts_List = msgfromclient.split(":");
                    /*RECIVING MESSAGES FROM MULTICAST 1 FOR ASSIGNING PRPOSAL VALUES*/
                    if (msgfromclient_parts_List.length == 3) {
                        Log.d(TAG, "sever got s message" +msgfromclient);
                        String msg_Id = msgfromclient_parts_List[0];
                        String msg = msgfromclient_parts_List[1];
                        String [] s3=msg.split(" ");
                        String msg1=s3[0];
                        String fp=s3[1].toString();
                        String port_Number = msgfromclient_parts_List[2];
                        if (sa_Id .get()>= ss_Id.get()) {
                            Log.d(TAG, "server id for message s: " + ss_Id.get());
                            Log.d(TAG, "server id for message s: " + sa_Id.get());
                            ss_Id.set(sa_Id.get()+1);
                            Log.d(TAG, "server id after updation for message s: " + ss_Id.get());
                            Log.d(TAG, "server id after updation for message s: " + sa_Id.get());


                        } else {
                            Log.d(TAG, "server id for message s: " + ss_Id.get());
                            Log.d(TAG, "server id for message s: " + sa_Id.get());
                            ss_Id.set(ss_Id.get()+1);
                            Log.d(TAG, "server id after updation for message s: " + ss_Id.get());
                            Log.d(TAG, "server id after updation for message s: " + sa_Id.get());

                        }
                        String s = msg_Id + ":" + msg1 + ":" + port_Number + ":" + ss_Id.get() + ":" + "got_sp_ok";
                        message m = new message(msg_Id, msg, ss_Id.get(), port_Number,0);
                        /*ADDING MESSAGES IN INTILA QEUE WHICH ARE NOT FROM FAILED AVD(USING SYNCHRONISED SO THAT QUEUE IS ACESSED BY ONLY ONE THREAD AT ONCE)*/
                        if(!m.port_Number.equalsIgnoreCase(failed_Port) || !m.port_Number.equalsIgnoreCase(fp.trim())){
                            synchronized (pQueue){
                                pQueue.add(m);
                            }
                            output.println(s);

                        }


                        if (input.readLine().contains("iam_ok")) {

                            mysocket.close();
                        }
                        /* RECEIVING MAX AGREED SEQUENCE NUMBER AND STORING IN 2ND QUEUE FOR DELIVARY*/

                    } else if (msgfromclient_parts_List.length == 4) {

                        String msg_Id = msgfromclient_parts_List[0];
                        String msg = msgfromclient_parts_List[1];
                        String[] s3 = msg.split(" ");
                        String msg1 = s3[0];
                        String fp = s3[1].toString();
                        String port_Number = msgfromclient_parts_List[2];
                        sa_Id.set(Integer.parseInt(msgfromclient_parts_List[3]));
                        Log.d(TAG, "sever got max propsal message" + sa_Id);
                        message m = new message(msg_Id, msg1, sa_Id.get(), port_Number, 1);

                        Log.d(TAG, "Receiving msg from Client with max agreed number: " + msgfromclient);
                        synchronized (pQueue) {
                            pQueue.remove(m);
                            pQueue.add(m);
                            /*Log.d(TAG, "value of dd" + m.d + m.msg_Id);*/
                            Log.d(TAG, "adding message to priority queuue "+" "+m.msg);

                        }

                        for (message m4 : pQueue) {
                            Log.d(TAG, "m4++++" + m4.port_Number + ":" + fp + " " + failed_Port);
                            if (m4.port_Number.equalsIgnoreCase(fp.trim()) || m4.port_Number.equalsIgnoreCase(failed_Port)) {
                                Log.d(TAG, "removing failed port messages from queue");
                                synchronized (pQueue) {
                                    pQueue.remove(m4);

                                }

                            }
                        }


                        message head = pQueue.peek();
                        if (head != null) {
                            Log.d(TAG, "value of sd" + head.d + head.msg_Id);
                        }

                        while (head != null && head.d == 1) {
                            Log.d(TAG, "value of d" + head.d + head.msg_Id);
                            message m3 = pQueue.poll();
                            pQueue1.add(m3);
                            head = pQueue.peek();

                        }


                        /*BUFFERING MESSAGES BECAUSE I HAVE OBSERVED THAT MESSAGES ARE NOT GETTING DELIVERED TO ALL AVDS IN TH ORDER THEY SENT BECAUSE OF NETWORK DELAYS OR SOME OTHER REASON*/
                        if(pQueue1.size()>=20||c==1){
                            c=1;

                            message head1 = pQueue1.peek();
                                /*Log.d(TAG, "value of pQueu1 d  " + head.d + head.msg_Id);*/
                            while (head1 != null) {
                                message m4 = pQueue1.poll();
                                publishProgress(m4.msg);
                                head1 = pQueue1.peek();

                                }

                        }


                        output.println("got_sa_ok");
                        output.flush();
                        mysocket.close();
                        /*publishProgress(msgfromclient);*/
                    }


                }

            } catch (UnknownHostException e) {
                Log.e(TAG, "ServerTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ServerTask socket IOException");
            }
            return null;
        }

        protected void onProgressUpdate(String... strings) {
            /*
             * The following code displays what is received in doInBackground().
             */
            String strReceived = strings[0].trim();
            TextView remoteTextView = (TextView) findViewById(R.id.textView1);
            remoteTextView.append(strReceived + "\t\n");
            TextView localTextView = (TextView) findViewById(R.id.textView1);
            localTextView.append("\n");
            values.put(KEY_FIELD_DB, Integer.toString(primary_Key));
            values.put(VALUE_FIELD_DB, strReceived);
            contentResolver.insert(mUri, values);
            primary_Key++;
            return;
        }


    }


    private class ClientTask extends AsyncTask<String, Void, Void> {
        @Override
        protected Void doInBackground(String... params) {
            int proposal_ID = 0;
            int sequence_Number;
            String sender_Port = params[1];

            String msg_Id = params[0] + id;
            String msg = params[0];
            String msg1=params[0]+" "+failed_Port;


            List<Integer> l = new ArrayList();
            try {
                Socket client = null;
                for (String remote_port : REMOTE_PORT) {

                    client = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(remote_port));


                    String s = msg_Id + ":" + msg1 + ":" + params[1];
                    Log.d(TAG, "message string from client" + s);
                    String acc = "iam_ok";
                    PrintWriter msgToServer = new PrintWriter(client.getOutputStream(), true);
                    msgToServer.println(s);
                    Log.d(TAG, "message string sent to server from client1" + s);
                    msgToServer.flush();
                    try {
                        BufferedReader ackreader = new BufferedReader(new InputStreamReader(client.getInputStream()));
                        String ack = ackreader.readLine();
                        String[] ack_Parts = ack.split(":");
                        Log.d(TAG, "prosal ID from server " + ack_Parts[3]);
                        Log.d(TAG, "Receiving msg from Client with proposal id : " + ack);
                        l.add(Integer.parseInt(ack_Parts[3]));
                        Log.d(TAG, "lengh oflist l : " + l.size());
                        if (ack.contains("got_sp_ok")) {
                            Log.d(TAG, "sending acknowleg from client that recieved all proposals : " + l.size());
                            msgToServer.println(acc);
                            client.close();

                        }
                    } catch (IOException e) {
                        failed_Port = remote_port;
                        try {
                            Socket client1 = null;
                            for (String remote_port1 : REMOTE_PORT) {

                                client1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                        Integer.parseInt(remote_port1));
                                if (remote_port1.equals(failed_Port)) {
                                    Log.d(TAG, "failed port detected skipping current iteration1");

                                    client1.close();
                                    continue;
                                }
                                failed_Port=failed_Port = String.valueOf(client.getPort());
                                client1.close();
                            }
                        }catch (Exception e1){
                            e1.printStackTrace();
                        }
                        Log.e(TAG, e.toString());
                        client.close();
                    } catch (NullPointerException e) {
                        Log.d(TAG, "failure of port detected: " + String.valueOf(client.getPort()));
                        failed_Port = String.valueOf(client.getPort());
                        try {
                            Socket client1 = null;
                            for (String remote_port1 : REMOTE_PORT) {

                                client1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                        Integer.parseInt(remote_port1));
                                if (remote_port1.equals(failed_Port)) {
                                    Log.d(TAG, "failed port detected skipping current iteration1");

                                    client1.close();
                                    continue;
                                }
                                failed_Port=failed_Port = String.valueOf(client.getPort());
                                client1.close();
                            }
                        }catch (Exception e1){
                            e1.printStackTrace();
                        }
                        Log.e(TAG, e.toString());
                    }
                }
                Object max = Collections.max(l);
                String m = max.toString();
                Log.d(TAG, "maimum of propsals recieved: " + m);
                for (String remote_port : REMOTE_PORT) {
                    String s1 = msg_Id + ":" + msg1 + ":" + params[1] + ":" + m;
                    String s3 = "iam_ok";
                    Log.d(TAG, "mesage with max proposal number " + s1);
                    if (remote_port.equals(failed_Port)) {
                        Log.d(TAG, "failed port detected skipping current iteration");

                        client.close();
                        continue;
                    }
                    client = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(remote_port));


                    PrintWriter msgToServer1 = new PrintWriter(client.getOutputStream(), true);
                    BufferedReader ackreader2 = new BufferedReader(new InputStreamReader(client.getInputStream()));
                    Log.d(TAG, "sending s1 to server " + s1);
                    msgToServer1.println(s1);
                    msgToServer1.flush();
                    try {
                        if (ackreader2.readLine().contains("got_sa_ok")) {
                            msgToServer1.println(s3);
                            client.close();

                        }
                    } catch (IOException e) {
                        client.close();
                        Log.e(TAG, e.toString());
                    }catch (NullPointerException e) {
                        client.close();

                    }
                }
                id++;


            } catch (IOException e) {
                /*failed_Port = String.valueOf(client.getPort());*/
                Log.e(TAG, e.toString());

            } catch (NullPointerException e) {
                /* failed_Port = String.valueOf(client.getPort());*/
                Log.e(TAG, e.toString());
            }
            return null;

        }
    }
}
