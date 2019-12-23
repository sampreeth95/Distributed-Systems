package edu.buffalo.cse.cse486586.groupmessenger1;

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
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Dictionary;
import java.util.HashMap;
import java.util.List;

/**
 * GroupMessengerActivity is the main Activity for the assignment.
 *
 * @author stevko
 *
 */
public class GroupMessengerActivity extends Activity {
    static final String TAG = GroupMessengerActivity.class.getSimpleName();
    static final String[] REMOTE_PORT = {"11108", "11112", "11116", "11120", "11124"};
    static final int SERVER_PORT = 10000;
    private static final String KEY_FIELD_DB = "key";
    private static final String VALUE_FIELD_DB = "value";
    int primary_Key;
    public static int id=0;
    public static   int ss_Id=0;
    public static  int sa_Id=0;
    ContentValues values = new ContentValues();
    private Uri mUri;
    ContentResolver contentResolver;
    HashMap<String, Integer> d = new HashMap<String, Integer>();
    int i=0;

    class message{
        String msg_Id="";
        String msg="";
        int sa_Id=0;
        String port_Number="";

        public message(String msg_Id,String msg,int sa_Id,String port_Number){
            this.msg_Id=msg_Id;
            this.msg=msg;
            this.sa_Id=sa_Id;
            this.port_Number=port_Number;
        }

    }
    PriorityQueue<message> pQueue =
            new PriorityQueue<message>(20,new messageComparator());


    class messageComparator implements Comparator<message> {

        // Overriding compare()method of Comparator
        // for descending order of cgpa
        public int compare(message m1, message m2) {
            if (m1.sa_Id < m2.sa_Id) {
                return 1;
            } else if (m1.sa_Id > m2.sa_Id) {
                return -1;
            }

            return 0;
        }


    }



    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority("edu.buffalo.cse.cse486586.groupmessenger1.provider");
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
                    PrintWriter output = new PrintWriter(mysocket.getOutputStream(), true);
                    BufferedReader input = new BufferedReader(new InputStreamReader(mysocket.getInputStream()));
                    msgfromclient = input.readLine();
                    String[] msgfromclient_parts_List = msgfromclient.split(":");
                    if(msgfromclient_parts_List.length==3){
                        String msg_Id=msgfromclient_parts_List[0];
                        String msg=msgfromclient_parts_List[1];
                        String port_Number=msgfromclient_parts_List[2];
                        if(sa_Id>=ss_Id){
                            ss_Id=sa_Id+1;
                            Log.d(TAG, "Receiving msg from Clientggggg1 : " + ss_Id);

                        }
                        else{
                            ss_Id=ss_Id+1;
                            Log.d(TAG, "Receiving msg from Clientggggg2 : " + ss_Id);
                        }
                        Log.d(TAG, "Receiving msg from Clientggggg : " + ss_Id);
                        String s=msg_Id+":"+msg+":"+port_Number+":"+ss_Id+":"+"got_sp_ok";
                        output.println(s);
                        if(input.readLine().contains("iam_ok")){
                            mysocket.close();
                        }

                    }
                    else if(msgfromclient_parts_List.length==4){
                        String msg_Id=msgfromclient_parts_List[0];
                        String msg=msgfromclient_parts_List[1];
                        String port_Number=msgfromclient_parts_List[2];
                        sa_Id=Integer.parseInt(msgfromclient_parts_List[3]);
                        message m=new message(msg_Id,msg,sa_Id,port_Number
                        );

                        Log.d(TAG, "Receiving msg from Client4 : " + msgfromclient);
                        output.println("got_sa_ok");
                        if(input.readLine().contains("iam_ok")){

                            mysocket.close();
                            /*publishProgress(msgfromclient);*/
                            synchronized (pQueue){
                                pQueue.add(m);;


                            }

                        }


                    }
                    if (pQueue.size()>5){
                        for(i=0;i<=pQueue.size();i++){
                            message m3=pQueue.poll();



                            output.println("got_sa_ok");
                            publishProgress(m3.msg);
                        }
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
            int proposal_ID=0;
            int sequence_Number;
            String sender_Port = params[1];
            Socket client=null;
            String msg_Id=params[0]+id;
            String msg=params[0];

            ArrayList<Integer> l=new ArrayList<Integer>();
            for (String remote_port : REMOTE_PORT) {

                try {

                    if (remote_port.equals("11108")) {
                        client = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(remote_port));
                    }
                    if (remote_port.equals("11112")) {
                        client = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(remote_port));

                    }
                    if (remote_port.equals("11116")) {
                        client = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(remote_port));
                    }
                    if (remote_port.equals("11120")) {
                        client = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(remote_port));
                    }
                    if (remote_port.equals("11124")) {
                        client = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(remote_port));

                    }

                    String s = msg_Id+":"+msg+":"+params[1];
                    String acc="iam_ok";
                    PrintWriter msgToServer = new PrintWriter(client.getOutputStream(), true);
                    BufferedReader ackreader = new BufferedReader(new InputStreamReader(client.getInputStream()));
                    msgToServer.println(s);
                    msgToServer.flush();

                    String ack = ackreader.readLine();
                    String[] ack_Parts=ack.split(":");
                    Log.d(TAG, "sa from server " + ack_Parts[3]);
                    Log.d(TAG, "Receiving msg from Clientggggg12 : " +ack_Parts[3]);
                    l.add(Integer.parseInt(ack_Parts[3]));
                    Log.d(TAG, "Receiving msg from Clientggggg13 : " +ack_Parts.length);

                    if(ack.contains("got_sp_ok")){
                        msgToServer.println(acc);
                        client.close();

                    }

                } catch (Exception e) {
                    Log.e(TAG, e.toString());

                }
            }
            for (String remote_port : REMOTE_PORT) {
                Object max=Collections.max(l);
                String m=max.toString();
                String s1= msg_Id+":"+msg+":"+params[1]+":"+m;
                String s3="iam_ok";
                try{
                    client = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(remote_port));
                    PrintWriter msgToServer1 = new PrintWriter(client.getOutputStream(), true);
                    BufferedReader ackreader2 = new BufferedReader(new InputStreamReader(client.getInputStream()));
                    msgToServer1.println(s1);
                    msgToServer1.flush();
                    if(ackreader2.readLine().contains("got_sa_ok")){
                        msgToServer1.println(s3);
                        client.close();

                    }


                } catch (Exception e) {
                    Log.e(TAG, e.toString());


                }
            }
            id++;

            return null;

        }
    }
}