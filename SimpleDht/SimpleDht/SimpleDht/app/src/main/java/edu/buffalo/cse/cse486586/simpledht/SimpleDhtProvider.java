package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.LinkedList;
import java.util.TreeMap;

import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.database.sqlite.SQLiteQueryBuilder;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;


import static android.content.ContentValues.TAG;

public class SimpleDhtProvider extends ContentProvider {
    static  String myPort;
    static int SERVER_PORT=10000;
    String msg="";
    Node node1 =new Node();
    TreeMap< String,Node> map = new TreeMap<String,Node>();

    ContentValues values = new ContentValues();
    private static final String KEY_FIELD_DB = "key";
    private static final String VALUE_FIELD_DB = "value";
    ContentResolver contentResolver;

    public class  Node{
        String nodeId;
        String predNodeId;
        String succNodeId;
        String predPortNumber;
        String succPortNumber;
        String portNumber;

        public String getPortNumber(){
            return this.portNumber;

        }
        public void setPortNumber(String portNumber){
            this.portNumber=portNumber;

        }
        public String getNodeId(){
            return this.nodeId;

        }
        public void setNodeId(String nodeId){
            this.nodeId=nodeId;

        }
        public String getPredNodeId(){
            return this.predNodeId;

        }
        public void setPredNodeId(String predNodeId){
            this.predNodeId=predNodeId;

        }
        public String getSuccNodeId(){
            return this.succNodeId;

        }
        public void setSuccNodeId(String succNodeId){
            this.succNodeId=succNodeId;

        }
        public String getPredPortNumber(){
            return this.predPortNumber;

        }
        public void setPredPortNumber(String predPortNumber){
            this.predPortNumber=predPortNumber;

        }
        public String getSuccPortNumber(){
            return this.succPortNumber;

        }
        public void setSuccPortNumber(String succPortNumber){
            this.succPortNumber=succPortNumber;

        }

    }

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }
    private Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");

    public class DhtHelper extends SQLiteOpenHelper
    {

        private static final String DATABASE_NAME = "groupTable";
        private static final int DATABASE_VERSION = 1;
        public static final String key = "key";
        public static final String value = "value";

        private static final String TAG = "GroupMessengerDB";
        public static final String TABLE = "Dht1";
        public static final String TABLE1 = "Dht2";
        public static final String TABLE2 = "Dht3";

        DhtHelper(Context context) {
            super(context, DATABASE_NAME, null, DATABASE_VERSION);
        }

        @Override
        public void onCreate(SQLiteDatabase db) {
            db.execSQL("CREATE TABLE if not exists " + TABLE + " (" +
                    key + " ," +
                    value + ");");
            db.execSQL("CREATE TABLE if not exists " + TABLE1 + " (" +
                    key + " ," +
                    value + ");");
            db.execSQL("CREATE TABLE if not exists " + TABLE2 + " (" +
                    key + " ," +
                    value + ");");

        }

        @Override
        public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
            Log.w(TAG, "Upgrading db from  " + oldVersion + " to "
                    + newVersion + ", delete old table");
            db.execSQL("DROP TABLE IF EXISTS " + TABLE);
            onCreate(db);
        }

    }


    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        String key = selection;
        String[] s5={selection};
        String s4 = DhtHelper.key + " = ?";

        if (node1.getNodeId() == null || node1.getNodeId().equalsIgnoreCase(node1.getPredNodeId()) || selection.contains("@")) {
            SQLiteDatabase db = dbHelper.getReadableDatabase();
            SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();
            queryBuilder.setTables(DhtHelper.TABLE);
            if (selection.contains("@")) {
                db.delete(DhtHelper.TABLE, null, null);


            } else if (selection.contains("*")) {
                db.delete(DhtHelper.TABLE, null, null);
            } else {
                String s1 = DhtHelper.key + " like '" + selection + "'";
                Log.v("Query selection string", s1);
                db.delete(DhtHelper.TABLE, s4, s5);
            }

        } else if (selection.contains("*")) {
            Socket a;
            SQLiteDatabase db = dbHelper.getReadableDatabase();
            SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();
            queryBuilder.setTables(DhtHelper.TABLE);
            db.delete(DhtHelper.TABLE, null, null);
            try {
                a = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(node1.getSuccPortNumber()));

                PrintWriter msgToServer1 = new PrintWriter(a.getOutputStream(), true);
                msgToServer1.write(node1.predNodeId+ ":" + "delete");
                msgToServer1.println();
                msgToServer1.flush();
                Thread.sleep(100);
                a.close();
            } catch (Exception e) {
                e.printStackTrace();
            }


            // TODO Auto-generated method stub

        }
        else{
            String Keyhash="";
            String [] s3={selection};
            try{
                Keyhash=genHash(key);
                Log.i("Key hash: ",Keyhash);
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
            Log.i(TAG, "delete particular key  "+node1.getPortNumber()+node1.getSuccPortNumber());
            if(myPort.equalsIgnoreCase(node1.getSuccPortNumber()) || node1.getNodeId()==null){
                Log.i("In search of node sn",key+"in node   "+node1.getPortNumber());
                SQLiteDatabase db = dbHelper.getReadableDatabase();
                SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();
                queryBuilder.setTables(DhtHelper.TABLE);
                db.delete(DhtHelper.TABLE, s4, s5);

            }
            else if(node1.getNodeId().compareTo(node1.getPredNodeId())<0){
                if(Keyhash.compareTo(node1.getNodeId())<=0 || Keyhash.compareTo(node1.getPredNodeId())>0){
                    Log.i("In delete of node sn1",key+"in node   "+node1.getPortNumber());
                    SQLiteDatabase db = dbHelper.getReadableDatabase();
                    SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();
                    queryBuilder.setTables(DhtHelper.TABLE);
                    db.delete(DhtHelper.TABLE, s4, s5);;
                }
                else if(Keyhash.compareTo(node1.getNodeId())>0 || (Keyhash.compareTo(node1.getPredNodeId())<0 &&Keyhash.compareTo(node1.getNodeId())<0)){
                    Socket is;
                    try{
                        Log.i("In delete of node sn",key+"in node   "+node1.getPortNumber()+"sending to succesor node"+node1.getSuccPortNumber());
                        String msg1="QueryKey";
                        is= new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(node1.getSuccPortNumber()));
                        PrintWriter msgToServer1 = new PrintWriter(is.getOutputStream(), true);
                        msgToServer1.write(selection+":"+"delpkey");
                        msgToServer1.println();
                        msgToServer1.flush();
                        Thread.sleep(100);
                        is.close();
                    }catch (Exception e){
                        e.printStackTrace();
                    }

                }
            }
            else{
                if(Keyhash.compareTo(node1.getNodeId())<=0 && Keyhash.compareTo(node1.getPredNodeId())>0){
                    Log.i("In delete of node sn2",key+"in node   "+node1.getPortNumber());
                    SQLiteDatabase db = dbHelper.getReadableDatabase();
                    SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();
                    queryBuilder.setTables(DhtHelper.TABLE);
                    db.delete(DhtHelper.TABLE, s4, s5);
                }
                else if(Keyhash.compareTo(node1.getNodeId())>0 || (Keyhash.compareTo(node1.getPredNodeId())<0 &&Keyhash.compareTo(node1.getNodeId())<0)){
                    Socket is;
                    try{
                        String msg1="QueryKey";
                        Log.i("In delete of node sn3",key+"in node   "+node1.getPortNumber()+"sending to succesor node"+node1.getSuccPortNumber());
                        is= new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(node1.getSuccPortNumber()));
                        PrintWriter msgToServer1 = new PrintWriter(is.getOutputStream(), true);
                        msgToServer1.write(selection+":"+"delpkey");
                        msgToServer1.println();
                        msgToServer1.flush();
                        Thread.sleep(100);
                        is.close();
                    }catch (Exception e){
                        e.printStackTrace();
                    }
                }
            }
        }
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub
        Log.i(TAG, "knowing port numbers of node "+node1.getPortNumber()+node1.getSuccPortNumber()+node1.getPredPortNumber());
        SQLiteDatabase db = dbHelper.getWritableDatabase();
        Log.d(TAG, values.toString());
        String key=(String)values.get("key");
        String key_value=(String)values.get("value");
        String Keyhash="";
        Log.i("In Insert: ",key);
        try{
            Keyhash=genHash(key);
            Log.i("Key hash: ",Keyhash);
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        Log.i(TAG, "insert:   "+node1.getPortNumber()+node1.getSuccPortNumber());
        if(myPort.equalsIgnoreCase(node1.getSuccPortNumber()) || node1.getNodeId()==null){
            db.insert(DhtHelper.TABLE, null, values);
            Log.i("In Insert6: ",key);
        }
        else if(node1.getNodeId().compareTo(node1.getPredNodeId())<0){
            if(Keyhash.compareTo(node1.getNodeId())<=0 || Keyhash.compareTo(node1.getPredNodeId())>0){
                db.insert(DhtHelper.TABLE, null, values);
                Log.i("In Insert7: ",key);
            }
            else if(Keyhash.compareTo(node1.getNodeId())>0 || (Keyhash.compareTo(node1.getPredNodeId())<0 &&Keyhash.compareTo(node1.getNodeId())<0)){
                Socket is;
                try{
                    String msg1="insert";
                    Log.i("In Insert8: ",key);
                    is= new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(node1.getSuccPortNumber()));
                    PrintWriter msgToServer1 = new PrintWriter(is.getOutputStream(), true);
                    msgToServer1.write(msg1+":"+node1.getPortNumber()+":"+key+":"+key_value);
                    msgToServer1.println();
                    msgToServer1.flush();

                    BufferedReader ackreader2 = new BufferedReader(new InputStreamReader(is.getInputStream()));
                    if (ackreader2.readLine().contains("ACK OK"))
                    {
                        Log.i(TAG,"Successor sending. Insert Successful..");
                        is.close();
                    }
                }catch (Exception e){
                    e.printStackTrace();
                }
            }
        }
        else{
            if(Keyhash.compareTo(node1.getNodeId())<=0 && Keyhash.compareTo(node1.getPredNodeId())>0){
                Log.i("In Insert9: ",key);
                db.insert(DhtHelper.TABLE, null, values);
            }
            else if(Keyhash.compareTo(node1.getNodeId())>0 || (Keyhash.compareTo(node1.getPredNodeId())<0 &&Keyhash.compareTo(node1.getNodeId())<0)){
                Socket is;
                try{
                    String msg1="insert";
                    Log.i("In Insert10: ",key);
                    is= new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(node1.getSuccPortNumber()));
                    PrintWriter msgToServer1 = new PrintWriter(is.getOutputStream(), true);
                    msgToServer1.write(msg1+":"+node1.getPortNumber()+":"+key+":"+key_value);
                    msgToServer1.println();
                    msgToServer1.flush();
                    BufferedReader ackreader2 = new BufferedReader(new InputStreamReader(is.getInputStream()));
                    if (ackreader2.readLine().contains("ACK OK"))
                    {
                        Log.i(TAG,"Successor sending. Insert Successful..");
                        is.close();
                    }
                }catch (Exception e){
                    e.printStackTrace();
                }
            }
        }
        return null;
    }
    DhtHelper dbHelper;
    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        dbHelper=new DhtHelper(getContext());

        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "server not able create ServerSocket");
        }
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,msg,myPort);
        Log.i(TAG,"Client created");
        return false;
    }
    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            try {
                Socket socket = null;
                socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt("11108"));
                String portNumber = msgs[1];
                int pNumber = Integer.parseInt(portNumber);
                pNumber = pNumber / 2;
                portNumber = String.valueOf(pNumber);
                String msgToSend = "Join";
                Log.i("Inside Client Task: ", "Msg to be sent: " + msgToSend + " " + portNumber);
                PrintWriter msgToServer = new PrintWriter(socket.getOutputStream(), true);
                msgToServer.write(msgToSend + ":" + portNumber);
                msgToServer.println();
                msgToServer.flush();
                Log.i("Inside Client task", "Msg sent to server for join request");
                BufferedReader readline = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                String ack="";
                ack=readline.readLine();
                Log.i(TAG, "inside client"+ack);
                if(ack!=null) {
                    if (ack.contains("ACK OK")) {
                        Log.i(TAG, "server responded to create join request");
                        socket.close();
                    }
                }

            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTaskException");
            } catch (IOException e) {
                Log.e(TAG, "ClientTaskIOException"+e);
            }

            return null;
        }
    }
    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            String msgFromclient = "";
            Socket server;
            try{
                while(true) {
                    server = serverSocket.accept();
                    BufferedReader read = new BufferedReader(new InputStreamReader(server.getInputStream()));
                    msgFromclient = read.readLine();
                    Log.i(" Inside Server Task: ", "Request received- " + msgFromclient);
                    String[] MessageFromclient_Parts = msgFromclient.split(":");
                    if (msgFromclient.contains("Join") && myPort.equalsIgnoreCase("11108")) {
                        String message = MessageFromclient_Parts[0];
                        String pnumber = MessageFromclient_Parts[1];
                        Node node = new Node();
                        int portNumber = Integer.parseInt(pnumber);
                        portNumber=portNumber*2;
                        node.setPortNumber(String.valueOf(portNumber));
                        node.setNodeId(genHash(pnumber));
                        Log.i(" Inside Server Tsk: ", "Generated hash no for " + pnumber + ": " + node.getNodeId());
                        map.put(genHash(pnumber),node);
                        Log.i(TAG, "after creation of node" + node.getPortNumber() + " " + 23445 + " " + node.getNodeId());
                        String a;
                        a=map.keySet().toArray()[0].toString();
                        Log.i(TAG, "inside map" + map.size() + " " + map.get(a).getNodeId());
                        if(map.size()==1) {
                            a = map.keySet().toArray()[0].toString();
                            map.get(a).setSuccNodeId(map.get(a).getNodeId());
                            map.get(a).setPredNodeId(map.get(a).getNodeId());
                            map.get(a).setPredPortNumber(map.get(a).getPortNumber());
                            map.get(a).setSuccPortNumber(map.get(a).getPortNumber());
                        }
                        else if(map.size()==2){
                            String a1;
                            a1=map.keySet().toArray()[1].toString();
                            a = map.keySet().toArray()[0].toString();
                            map.get(a).setSuccNodeId(map.get(a1).getNodeId());
                            map.get(a).setPredNodeId(map.get(a1).getNodeId());
                            map.get(a).setPredPortNumber(map.get(a1).getPortNumber());
                            map.get(a).setSuccPortNumber(map.get(a1).getPortNumber());

                            map.get(a1).setSuccNodeId(map.get(a).getNodeId());
                            map.get(a1).setPredNodeId(map.get(a).getNodeId());
                            map.get(a1).setPredPortNumber(map.get(a).getPortNumber());
                            map.get(a1).setSuccPortNumber(map.get(a).getPortNumber());
                            }
                            else {
                            for (int i = 0; i < map.size(); i++) {
                                a = map.keySet().toArray()[i].toString();
                                if (i == 0) {
                                    map.get(a).setPredNodeId(map.get(map.lastKey()).getNodeId());
                                    map.get(a).setSuccNodeId(map.get(map.keySet().toArray()[i+1].toString()).getNodeId());

                                    map.get(a).setPredPortNumber(map.get(map.lastKey()).getPortNumber());
                                    map.get(a).setSuccPortNumber(map.get(map.keySet().toArray()[i+1].toString()).getPortNumber());

                                }
                                else if (i==map.size()-1){
                                    map.get(a).setPredNodeId(map.get(map.keySet().toArray()[i-1].toString()).getNodeId());
                                    map.get(a).setSuccNodeId(map.get(map.firstKey()).getNodeId());

                                    map.get(a).setPredPortNumber(map.get(map.keySet().toArray()[i-1].toString()).getPortNumber());
                                    map.get(a).setSuccPortNumber(map.get(map.firstKey()).getPortNumber());

                                }
                                else{
                                    map.get(a).setPredNodeId(map.get(map.keySet().toArray()[i-1].toString()).getNodeId());
                                    map.get(a).setSuccNodeId(map.get(map.keySet().toArray()[i+1].toString()).getNodeId());

                                    map.get(a).setPredPortNumber(map.get(map.keySet().toArray()[i-1].toString()).getPortNumber());
                                    map.get(a).setSuccPortNumber(map.get(map.keySet().toArray()[i+1].toString()).getPortNumber());

                                }
                            }
                        }

                        for (int j = 0; j < map.size(); j++) {
                            a = map.keySet().toArray()[j].toString();
                            if (map.get(a).getPortNumber().equals("11108")) {
                                node1.setPortNumber(map.get(a).getPortNumber());
                                node1.setNodeId(map.get(a).getNodeId());
                                node1.setPredNodeId(map.get(a).getPredNodeId());
                                node1.setSuccNodeId(map.get(a).getSuccNodeId());
                                node1.setPredPortNumber(map.get(a).getPredPortNumber());
                                node1.setSuccPortNumber(map.get(a).getSuccPortNumber());
                                continue;
                            }
                            Socket s = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(map.get(a).getPortNumber()));
                            PrintWriter msgToServer1 = new PrintWriter(s.getOutputStream(), true);
                            msgToServer1.write(map.get(a).getPortNumber() + ":" + map.get(a).getNodeId() + ":" + map.get(a).getPredNodeId() + ":" + map.get(a).getSuccNodeId() + ":" + map.get(a).getPredPortNumber() + ":" + map.get(a).getSuccPortNumber()+":"+"bradcast");
                            msgToServer1.println();
                            msgToServer1.flush();
                            BufferedReader ackreader2 = new BufferedReader(new InputStreamReader(s.getInputStream()));
                            String readLine = "";
                            readLine = ackreader2.readLine();
                            if (readLine.contains("ACK OK")) {
                                Log.i(TAG, "Server Communication Successful..");
                                s.close();
                            }
                        }
                    } else if (msgFromclient.contains("bradcast"))
                    {
                        node1.setPortNumber(MessageFromclient_Parts[0]);
                        node1.setNodeId(MessageFromclient_Parts[1]);
                        node1.setPredNodeId(MessageFromclient_Parts[2]);
                        node1.setSuccNodeId(MessageFromclient_Parts[3]);
                        node1.setPredPortNumber(MessageFromclient_Parts[4]);
                        node1.setSuccPortNumber(MessageFromclient_Parts[5]);
                        Log.i("Sending ACK", "ACK OK");
                        PrintWriter output = new PrintWriter(server.getOutputStream(), true);
                        output.write("ACK OK");
                        output.println();
                        output.flush();
                        server.close();
                    }
                    else if(msgFromclient.contains("insert")){
                        Log.i("Parts:", "message  "+MessageFromclient_Parts[0] + "sendernode    " + MessageFromclient_Parts[1] + "key  " + MessageFromclient_Parts[2] + "keyvalue    " + MessageFromclient_Parts[3]);
                        String Keyhash="";
                        SQLiteDatabase db = dbHelper.getWritableDatabase();
                        Log.i("In Insert: ",MessageFromclient_Parts[2]);
                        String key=MessageFromclient_Parts[2];
                        String key_value=MessageFromclient_Parts[3];
                        values.put(KEY_FIELD_DB,key);
                        values.put(VALUE_FIELD_DB,key_value);
                        try{
                            Keyhash=genHash(MessageFromclient_Parts[2]);
                            Log.i("Key hash: ",Keyhash);
                        }
                        catch (Exception e)
                        {
                            e.printStackTrace();
                        }
                        if(node1.getPortNumber().equalsIgnoreCase(node1.getSuccPortNumber())|| node1.getNodeId()==null){

                            Log.i("In Insert1: ",MessageFromclient_Parts[2]);
                            db.insert(DhtHelper.TABLE, null, values);

                        }
                        else if(node1.getNodeId().compareTo(node1.getPredNodeId())<0){
                            Log.i("In Insert23: ",MessageFromclient_Parts[2]);
                            if(Keyhash.compareTo(node1.getNodeId())<=0 || Keyhash.compareTo(node1.getPredNodeId())>0){
                                Log.i("In Insert2: ",MessageFromclient_Parts[2]);
                                db.insert(DhtHelper.TABLE, null, values);

                            }
                            else if(Keyhash.compareTo(node1.getNodeId())>0 || (Keyhash.compareTo(node1.getPredNodeId())<0 &&Keyhash.compareTo(node1.getNodeId())<0)){
                                Socket is;
                                try{
                                    String msg1="insert";
                                    Log.i("In Insert3: ",MessageFromclient_Parts[2]);
                                    is= new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(node1.getSuccPortNumber()));
                                    PrintWriter msgToServer1 = new PrintWriter(is.getOutputStream(), true);
                                    msgToServer1.write(msg1+":"+node1.getPortNumber()+":"+key+":"+key_value);
                                    msgToServer1.println();
                                    msgToServer1.flush();
                                    BufferedReader ackreader2 = new BufferedReader(new InputStreamReader(is.getInputStream()));
                                    if (ackreader2.readLine().contains("ACK OK"))
                                    {
                                        Log.i(TAG,"Successor sending. Insert Successful");
                                        is.close();
                                    }
                                }catch (Exception e){
                                    e.printStackTrace();
                                }
                            }
                        }
                        else{
                            Log.i("In Insert24: ",MessageFromclient_Parts[2]);
                            if(Keyhash.compareTo(node1.getNodeId())<=0 && Keyhash.compareTo(node1.getPredNodeId())>0){

                                Log.i("In Insert4: ",MessageFromclient_Parts[2]);
                                db.insert(DhtHelper.TABLE, null, values);
                            }
                            else if(Keyhash.compareTo(node1.getNodeId())>0 || (Keyhash.compareTo(node1.getPredNodeId())<0 &&Keyhash.compareTo(node1.getNodeId())<0)){
                                Socket is;
                                try{
                                    String msg1="insert";
                                    Log.i("In Insert5: ",MessageFromclient_Parts[2]);
                                    is= new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(node1.getSuccPortNumber()));
                                    PrintWriter msgToServer1 = new PrintWriter(is.getOutputStream(), true);
                                    msgToServer1.write(msg1+":"+node1.getPortNumber()+":"+key+":"+key_value);
                                    msgToServer1.println();
                                    msgToServer1.flush();
                                    BufferedReader ackreader2 = new BufferedReader(new InputStreamReader(is.getInputStream()));
                                    if (ackreader2.readLine().contains("ACK OK"))
                                    {
                                        Log.i(TAG,"Successor sending. Insert Successful");
                                        is.close();
                                    }
                                }catch (Exception e){
                                    e.printStackTrace();
                                }
                            }
                        }
                        Log.i("Sending ACK", "ACK OK");
                        OutputStreamWriter osw = new OutputStreamWriter(server.getOutputStream());
                        BufferedWriter bwServer = new BufferedWriter(osw);
                        bwServer.write("ACK OK");
                        bwServer.newLine();
                        bwServer.flush();
                        server.close();
                    }
                    else if(MessageFromclient_Parts[0].equalsIgnoreCase("all")){
                        Log.i("In all_query ",node1.getPortNumber());
                        String q=MessageFromclient_Parts[0];
                        String predoforignalNode=MessageFromclient_Parts[1];
                        SQLiteDatabase db = dbHelper.getReadableDatabase();
                        SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();
                        queryBuilder.setTables(DhtHelper.TABLE);
                        if(node1.getNodeId().equalsIgnoreCase(predoforignalNode)){
                            Log.i("In all_query12 ",node1.getPortNumber());
                            Cursor cursor = queryBuilder.query(db, null, null,
                                    null, null, null, null);
                            String kvinthisnode="";
                            String temp="";
                            if(cursor.moveToFirst()){
                                while(!cursor.isAfterLast()){
                                    if(cursor.isLast()) {
                                        temp=cursor.getString(0)+" "+cursor.getString(1);
                                    }
                                    else{
                                        temp=cursor.getString(0)+" "+cursor.getString(1)+" ";
                                    }
                                    kvinthisnode+=temp;
                                    cursor.moveToNext();
                                }
                                Log.i("In all_query ",kvinthisnode);
                                PrintWriter msgToServer1 = new PrintWriter(server.getOutputStream(), true);
                                msgToServer1.write(kvinthisnode);
                                msgToServer1.println();
                                msgToServer1.flush();
                            }
                            else{
                                Log.i("In all_query13 ",kvinthisnode);

                                PrintWriter msgToServer1 = new PrintWriter(server.getOutputStream(), true);
                                msgToServer1.write("Nomessages"+" "+"ACKOK");
                                msgToServer1.println();
                                msgToServer1.flush();
                            }
                            server.close();
                        }
                        else{
                            Log.i("In all_query6",node1.getPortNumber());
                            Socket a1;
                            try{
                                a1= new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(node1.getSuccPortNumber()));
                                PrintWriter msgToServer1 = new PrintWriter(a1.getOutputStream(), true);
                                msgToServer1.write(q+":"+predoforignalNode);
                                msgToServer1.println();
                                msgToServer1.flush();
                                BufferedReader ackreader2 = new BufferedReader(new InputStreamReader(a1.getInputStream()));
                                String s=ackreader2.readLine();
                                Log.i("In all_query1 ",s);
                                String [] sParts=s.split(" ");
                                if (sParts.length==2 &&sParts[0].equalsIgnoreCase("Nomessages")){
                                    if (s.contains("ACKOK")){
                                        a1.close();
                                    }
                                    Cursor cursor = queryBuilder.query(db, null, null,
                                            null, null, null, null);
                                    String kvinthisnode="";
                                    String temp="";
                                    if(cursor.moveToFirst()){
                                        while(!cursor.isAfterLast()){
                                            if(cursor.isLast()) {
                                                temp=cursor.getString(0)+" "+cursor.getString(1);
                                            }
                                            else{
                                                temp=cursor.getString(0)+" "+cursor.getString(1)+" ";
                                            }
                                            kvinthisnode+=temp;
                                            cursor.moveToNext();
                                        }
                                        Log.i("In all_query2 ",kvinthisnode);
                                        PrintWriter msgToServer2 = new PrintWriter(server.getOutputStream(), true);
                                        msgToServer2.write(kvinthisnode.trim());
                                        msgToServer2.println();
                                        msgToServer2.flush();
                                    }
                                    else{
                                        PrintWriter msgToServer2 = new PrintWriter(server.getOutputStream(), true);
                                        msgToServer2.write("Nomessages"+" "+"ACKOK");
                                        msgToServer2.println();
                                        msgToServer2.flush();
                                    }
                                    server.close();
                                }
                                else {
                                    Log.i("In all_query7 ",node1.getPortNumber()+"  "+s);
                                    Cursor cursor = queryBuilder.query(db, null, null,
                                            null, null, null, null);

                                    String kvinthisnode=s+" ";
                                    String temp="";
                                    Log.i("In all_query3 ",kvinthisnode);
                                    if(cursor.moveToFirst()){
                                        while(!cursor.isAfterLast()){
                                            if(cursor.isLast()) {
                                                temp=cursor.getString(0)+" "+cursor.getString(1);
                                            }
                                            else{
                                                temp=cursor.getString(0)+" "+cursor.getString(1)+" ";
                                            }
                                            kvinthisnode+=temp;
                                            cursor.moveToNext();
                                        }
                                        Log.i("In all_query4 ",kvinthisnode);
                                        PrintWriter msgToServer2 = new PrintWriter(server.getOutputStream(), true);
                                        msgToServer2.write(kvinthisnode.trim());
                                        msgToServer2.println();
                                        msgToServer2.flush();
                                        server.close();
                                    }
                                    else{
                                        Log.i("In all_query5 ",kvinthisnode);
                                        PrintWriter msgToServer2 = new PrintWriter(server.getOutputStream(), true);
                                        msgToServer2.write(kvinthisnode.trim());
                                        msgToServer2.println();
                                        msgToServer2.flush();
                                        server.close();
                                    }
                                }
                            }catch (Exception e){
                                e.printStackTrace();
                            }
                        }
                    }
                    else if(msgFromclient.contains("QueryKey")){
                        Log.i("Parts:", "message  "+MessageFromclient_Parts[0] + "sendernode " + MessageFromclient_Parts[1] + "key  " + MessageFromclient_Parts[2] + "keyvalue    " + MessageFromclient_Parts[3]);
                        String Keyhash="";
                        SQLiteDatabase db = dbHelper.getWritableDatabase();
                        Log.i("In search of :   ",MessageFromclient_Parts[2]+"in node   "+node1.getPortNumber());
                        String key=MessageFromclient_Parts[2];
                        String key_value=MessageFromclient_Parts[3];
                        String [] s3={key};
                        String s4=DhtHelper.key+ " = ?";
                        SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();
                        queryBuilder.setTables(DhtHelper.TABLE);

                        try{
                            Keyhash=genHash(MessageFromclient_Parts[2]);
                            Log.i("Key hash: ",Keyhash);
                        }
                        catch (Exception e)
                        {
                            e.printStackTrace();
                        }
                        if(node1.getPortNumber().equalsIgnoreCase(node1.getSuccPortNumber())|| node1.getNodeId()==null){
                            Log.i("In search of1 :   ",MessageFromclient_Parts[2]+"in node   "+node1.getPortNumber());
                            Cursor cursor = queryBuilder.query(db, null, s4,
                                    s3, null, null, null);
                            String kvinthisnode="";
                            if(cursor.moveToFirst()){
                                kvinthisnode=cursor.getString(0)+"#"+cursor.getString(1);
                                Log.i("In search of 11:   ",kvinthisnode+"in node   "+node1.getPortNumber());
                                PrintWriter msgToServer1 = new PrintWriter(server.getOutputStream(), true);
                                msgToServer1.write(kvinthisnode+"#"+"ACK OK");
                                msgToServer1.println();
                                msgToServer1.flush();
                            }
                            server.close();
                        }
                        else if(node1.getNodeId().compareTo(node1.getPredNodeId())<0){
                            Log.i("In search of 2:   ",MessageFromclient_Parts[2]+"in node   "+node1.getPortNumber());
                            if(Keyhash.compareTo(node1.getNodeId())<=0 || Keyhash.compareTo(node1.getPredNodeId())>0){
                                Log.i("In search of 3:   ",MessageFromclient_Parts[2]+"in node   "+node1.getPortNumber());
                                Cursor cursor = queryBuilder.query(db, null, s4,
                                        s3, null, null, null);
                                String kvinthisnode="";
                                if(cursor.moveToFirst()){
                                    kvinthisnode=cursor.getString(0)+"#"+cursor.getString(1);
                                    Log.i("In search of 31:   ",kvinthisnode+"in node   "+node1.getPortNumber());
                                    PrintWriter msgToServer1 = new PrintWriter(server.getOutputStream(), true);
                                    msgToServer1.write(kvinthisnode+"#"+"ACK OK");
                                    msgToServer1.println();
                                    msgToServer1.flush();

                                }
                                server.close();
                            }
                            else if(Keyhash.compareTo(node1.getNodeId())>0 || (Keyhash.compareTo(node1.getPredNodeId())<0 &&Keyhash.compareTo(node1.getNodeId())<0)){
                                Socket is;
                                try{
                                    Log.i("In search of4 :   ",MessageFromclient_Parts[2]+"in node   "+node1.getPortNumber()+"sending to  "+node1.getSuccPortNumber());
                                    String msg1="QueryKey";
                                    is= new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(node1.getSuccPortNumber()));
                                    PrintWriter msgToServer1 = new PrintWriter(is.getOutputStream(), true);
                                    msgToServer1.write(msg1+":"+node1.getPortNumber()+":"+key+":"+key_value);
                                    msgToServer1.println();
                                    msgToServer1.flush();
                                    BufferedReader ackreader2 = new BufferedReader(new InputStreamReader(is.getInputStream()));
                                    String x=ackreader2.readLine();
                                    if(x.contains("ACK OK")){
                                        PrintWriter msgToServer2 = new PrintWriter(server.getOutputStream(), true);
                                        msgToServer2.write(x);
                                        msgToServer2.println();
                                        msgToServer2.flush();
                                        is.close();
                                    }
                                    Log.i("In search of 41:   ",x+"in node   "+node1.getPortNumber());
                                }catch (Exception e){
                                    e.printStackTrace();
                                }
                                server.close();
                            }
                        }
                        else{
                            if(Keyhash.compareTo(node1.getNodeId())<=0 && Keyhash.compareTo(node1.getPredNodeId())>0){
                                Log.i("In search of 5:   ",MessageFromclient_Parts[2]+"in node   "+node1.getPortNumber());
                                String kvinthisnode="";
                                Cursor cursor = queryBuilder.query(db, null, s4,
                                        s3, null, null, null);
                                if(cursor.moveToFirst()){
                                    kvinthisnode=cursor.getString(0)+"#"+cursor.getString(1);
                                    Log.i("In search of 51:   ",kvinthisnode+"in node   "+node1.getPortNumber());
                                    PrintWriter msgToServer1 = new PrintWriter(server.getOutputStream(), true);
                                    msgToServer1.write(kvinthisnode+"#"+"ACK OK");
                                    msgToServer1.println();
                                    msgToServer1.flush();
                                }
                                server.close();
                            }
                            else if(Keyhash.compareTo(node1.getNodeId())>0 || (Keyhash.compareTo(node1.getPredNodeId())<0 &&Keyhash.compareTo(node1.getNodeId())<0)){
                                Socket is;
                                try{
                                    String msg1="QueryKey";
                                    Log.i("In search of 6:   ",MessageFromclient_Parts[2]+"in node   "+node1.getPortNumber()+"sending to next node   "+node1.getSuccPortNumber());
                                    is= new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(node1.getSuccPortNumber()));
                                    PrintWriter msgToServer1 = new PrintWriter(is.getOutputStream(), true);
                                    msgToServer1.write(msg1+":"+node1.getPortNumber()+":"+key+":"+key_value);
                                    msgToServer1.println();
                                    msgToServer1.flush();
                                    BufferedReader ackreader2 = new BufferedReader(new InputStreamReader(is.getInputStream()));
                                    String x=ackreader2.readLine();
                                    if(x.contains("ACK OK")){

                                        PrintWriter msgToServer2 = new PrintWriter(server.getOutputStream(), true);
                                        msgToServer2.write(x);
                                        msgToServer2.println();
                                        msgToServer2.flush();
                                        is.close();
                                    }
                                    Log.i("In search of 6:   ",x+"in node   "+node1.getPortNumber()+"sending to next node   "+node1.getSuccPortNumber());

                                }catch (Exception e){
                                    e.printStackTrace();
                                }
                                server.close();
                            }
                        }
                    }
                    else if(MessageFromclient_Parts[1].equalsIgnoreCase("delete")){
                        if(node1.getNodeId().equalsIgnoreCase(MessageFromclient_Parts[0])){
                            SQLiteDatabase db = dbHelper.getReadableDatabase();
                            SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();
                            queryBuilder.setTables(DhtHelper.TABLE);
                            db.delete(DhtHelper.TABLE, null, null);
                        }
                        else{
                            SQLiteDatabase db = dbHelper.getReadableDatabase();
                            SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();
                            queryBuilder.setTables(DhtHelper.TABLE);
                            db.delete(DhtHelper.TABLE, null, null);
                            Socket s;
                            try {
                                s= new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(node1.getSuccPortNumber()));
                                PrintWriter msgToServer1 = new PrintWriter(s.getOutputStream(), true);
                                msgToServer1.write(MessageFromclient_Parts[0]+":"+"delete");
                                msgToServer1.println();
                                msgToServer1.flush();
                                s.close();
                            }catch (Exception e){
                                e.printStackTrace();
                            }
                        }
                    }
                    else if(MessageFromclient_Parts[1].equalsIgnoreCase("delpkey")){
                        String Keyhash="";
                        SQLiteDatabase db = dbHelper.getWritableDatabase();
                        Log.i("In delete of :   ",MessageFromclient_Parts[0]+"in node   "+node1.getPortNumber());
                        String key=MessageFromclient_Parts[0];
                        String [] s3={key};
                        String s4=DhtHelper.key+ " = ?";
                        SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();
                        queryBuilder.setTables(DhtHelper.TABLE);
                        try{
                            Keyhash=genHash(MessageFromclient_Parts[0]);
                            Log.i("Key hash: ",Keyhash);
                        }
                        catch (Exception e)
                        {
                            e.printStackTrace();
                        }
                        if(node1.getPortNumber().equalsIgnoreCase(node1.getSuccPortNumber())|| node1.getNodeId()==null){
                            Log.i("In delete of1 :   ",MessageFromclient_Parts[0]+"in node   "+node1.getPortNumber());
                            db.delete(DhtHelper.TABLE, s4, s3);
                            server.close();
                        }
                        else if(node1.getNodeId().compareTo(node1.getPredNodeId())<0){
                            Log.i("In delete  of 2:   ",MessageFromclient_Parts[0]+"in node   "+node1.getPortNumber());
                            if(Keyhash.compareTo(node1.getNodeId())<=0 || Keyhash.compareTo(node1.getPredNodeId())>0){
                                Log.i("In delete of 3:   ",MessageFromclient_Parts[0]+"in node   "+node1.getPortNumber());
                                db.delete(DhtHelper.TABLE, s4, s3);
                                server.close();
                            }
                            else if(Keyhash.compareTo(node1.getNodeId())>0 || (Keyhash.compareTo(node1.getPredNodeId())<0 &&Keyhash.compareTo(node1.getNodeId())<0)){
                                Socket is;
                                try{
                                    Log.i("In delete of4 :   ",MessageFromclient_Parts[0]+"in node   "+node1.getPortNumber()+"sending to  "+node1.getSuccPortNumber());
                                    String msg1="QueryKey";
                                    is= new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(node1.getSuccPortNumber()));
                                    PrintWriter msgToServer1 = new PrintWriter(is.getOutputStream(), true);
                                    msgToServer1.write(key+":"+"delpkey");
                                    msgToServer1.println();
                                    msgToServer1.flush();
                                    Thread.sleep(100);
                                    is.close();
                                }catch (Exception e){
                                    e.printStackTrace();
                                }
                                server.close();
                            }
                        }
                        else{
                            if(Keyhash.compareTo(node1.getNodeId())<=0 && Keyhash.compareTo(node1.getPredNodeId())>0){
                                Log.i("In delete of 5:   ",MessageFromclient_Parts[0]+"in node   "+node1.getPortNumber());
                                db.delete(DhtHelper.TABLE, s4, s3);
                                server.close();
                            }
                            else if(Keyhash.compareTo(node1.getNodeId())>0 || (Keyhash.compareTo(node1.getPredNodeId())<0 &&Keyhash.compareTo(node1.getNodeId())<0)){
                                Socket is;
                                try{
                                    String msg1="QueryKey";
                                    Log.i("In delete of 6:   ",MessageFromclient_Parts[0]+"in node   "+node1.getPortNumber()+"sending to next node   "+node1.getSuccPortNumber());
                                    is= new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(node1.getSuccPortNumber()));
                                    PrintWriter msgToServer1 = new PrintWriter(is.getOutputStream(), true);
                                    msgToServer1.write(key+":"+"delpkey");
                                    msgToServer1.println();
                                    msgToServer1.flush();
                                    Thread.sleep(100);
                                    is.close();
                                }catch (Exception e){
                                    e.printStackTrace();
                                }
                                server.close();
                            }
                        }
                    }
                }
            }
            catch(Exception e)
            {
                e.printStackTrace();
            }
            return null;
        }
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        Cursor cursor=null;
        if (node1.getNodeId()==null||node1.getNodeId().equalsIgnoreCase(node1.getPredNodeId())||selection.contains("@")){
            SQLiteDatabase db = dbHelper.getReadableDatabase();
            SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();
            queryBuilder.setTables(DhtHelper.TABLE);
            if(selection.contains("@")){
                cursor = queryBuilder.query(db, null, null,
                        null, null, null, sortOrder);
            }
            else if(selection.contains("*")){
                cursor = queryBuilder.query(db, null, null,
                        null, null, null, sortOrder);
            }
            else{
                String s1=DhtHelper.key+" like '"+selection+"'";
                Log.v("Query selection string", s1);
                cursor = queryBuilder.query(db, null, s1, selectionArgs, null, null, sortOrder);
            }
        }
        else if(selection.contains("*")){
            Socket a;
            try{
                a= new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(node1.getSuccPortNumber()));
                PrintWriter msgToServer1 = new PrintWriter(a.getOutputStream(), true);
                msgToServer1.write("all"+":"+node1.getPredNodeId());
                msgToServer1.println();
                msgToServer1.flush();
                BufferedReader ackreader2 = new BufferedReader(new InputStreamReader(a.getInputStream()));
                String s1=ackreader2.readLine();
                s1=s1.trim();
                String [] s1Parts=s1.split(" ");
                SQLiteDatabase db = dbHelper.getReadableDatabase();
                SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();
                queryBuilder.setTables(DhtHelper.TABLE);
                Log.i("return",s1);
                if (s1Parts.length==2 &&s1Parts[0].equalsIgnoreCase("Nomessages")) {
                    if (s1.contains("ACK OK")) {
                        a.close();
                    }
                    cursor = queryBuilder.query(db, null, null,
                            null, null, null, null);
                }
                else{
                    Log.i("return1",s1);
                    String kvinthisnode = s1+" ";
                    String temp = "";
                    cursor = queryBuilder.query(db, null, null,
                            null, null, null, null);
                    if (cursor.moveToFirst()) {
                        while (!cursor.isAfterLast()) {
                            if (cursor.isLast()) {
                                temp = cursor.getString(0) + " " + cursor.getString(1);
                            } else {
                                temp = cursor.getString(0) + " " + cursor.getString(1) + " ";
                            }
                            kvinthisnode += temp;
                            cursor.moveToNext();
                        }
                        Log.i("return",kvinthisnode);
                        String [] s3Parts=kvinthisnode.split(" ");
                        LinkedList<String> lk=new LinkedList<String>();
                        LinkedList<String> lv=new LinkedList<String>();
                        for (int i=0;i<s3Parts.length;i=i+2){
                            lk.add(s3Parts[i]);
                        }
                        for (int i=1;i<s3Parts.length;i=i+2){
                            lv.add(s3Parts[i]);
                        }
                        Log.i("In sizes of lists",lk.size()+"   "+lv.size()+"   "+s3Parts.length);
                        for(int j=0;j<lk.size();j++){
                            String k=lk.get(j);
                            String v=lv.get(j);
                            values.put(KEY_FIELD_DB,k);
                            values.put(VALUE_FIELD_DB,v);
                            SQLiteDatabase db1 = dbHelper.getWritableDatabase();
                            queryBuilder.setTables(DhtHelper.TABLE1);

                            db1.insert(DhtHelper.TABLE1, null, values);
                            getContext().getContentResolver().notifyChange(uri, null);
                        }
                    }
                    else{
                        Log.i("return3",kvinthisnode);
                        String [] s3Parts=kvinthisnode.trim().split(" ");
                        Log.i("return3",kvinthisnode+"     "+s3Parts.length);
                        ArrayList<String> lk=new ArrayList<String>();
                        ArrayList<String> lv=new ArrayList<String>();
                        //LinkedList<String> lk=new LinkedList<String>();
                        //LinkedList<String> lv=new LinkedList<String>();
                        for (int i=0;i<s3Parts.length;i=i+2){
                            lk.add(s3Parts[i]);
                        }
                        for (int i=1;i<s3Parts.length;i=i+2){
                            lv.add(s3Parts[i]);
                        }
                        Log.i("In sizes of lists",lk.size()+"   "+lv.size()+"   "+s3Parts.length);
                        for(int j=0;j<lk.size();j++){
                            String k=lk.get((j));
                            String v=lv.get(j);
                            values.put(KEY_FIELD_DB,k);
                            values.put(VALUE_FIELD_DB,v);
                            SQLiteDatabase db1 = dbHelper.getWritableDatabase();
                            queryBuilder.setTables(DhtHelper.TABLE1);
                            db1.insert(DhtHelper.TABLE1, null, values);
                            getContext().getContentResolver().notifyChange(uri, null);
                        }
                    }
                }
                SQLiteDatabase db1 = dbHelper.getWritableDatabase();

                cursor = queryBuilder.query(db1, null, null,
                        null, null, null, null);
            }catch (Exception e){
                e.printStackTrace();
            }
            return  cursor;
        }
        else{
            SQLiteDatabase db2 = dbHelper.getWritableDatabase();

            String key=selection;
            String s4=DhtHelper.key+" = ?";
            String Keyhash="";
            String [] s3={selection};
            try{
                Keyhash=genHash(key);
                Log.i("Key hash: ",Keyhash);
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
            Log.i(TAG, "Query particular key  "+node1.getPortNumber()+node1.getSuccPortNumber());
            if(myPort.equalsIgnoreCase(node1.getSuccPortNumber()) || node1.getNodeId()==null){
                Log.i("In search of node sn",key+"in node   "+node1.getPortNumber());
                SQLiteDatabase db = dbHelper.getReadableDatabase();
                SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();
                queryBuilder.setTables(DhtHelper.TABLE);
                cursor = queryBuilder.query(db, null, s4,
                        s3, null, null, sortOrder);
            }
            else if(node1.getNodeId().compareTo(node1.getPredNodeId())<0){
                if(Keyhash.compareTo(node1.getNodeId())<=0 || Keyhash.compareTo(node1.getPredNodeId())>0){
                    Log.i("In search of node sn1",key+"in node   "+node1.getPortNumber());
                    SQLiteDatabase db = dbHelper.getReadableDatabase();
                    SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();
                    queryBuilder.setTables(DhtHelper.TABLE);
                    cursor = queryBuilder.query(db, null, s4,
                            s3, null, null, sortOrder);
                }
                else if(Keyhash.compareTo(node1.getNodeId())>0 || (Keyhash.compareTo(node1.getPredNodeId())<0 &&Keyhash.compareTo(node1.getNodeId())<0)){
                    Socket is;
                    try{
                        Log.i("In search of node sn",key+"in node   "+node1.getPortNumber()+"sending to succesor node"+node1.getSuccPortNumber());
                        String msg1="QueryKey";
                        is= new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(node1.getSuccPortNumber()));
                        PrintWriter msgToServer1 = new PrintWriter(is.getOutputStream(), true);
                        msgToServer1.write(msg1+":"+node1.getPortNumber()+":"+key+":"+key);
                        msgToServer1.println();
                        msgToServer1.flush();
                        BufferedReader ackreader2 = new BufferedReader(new InputStreamReader(is.getInputStream()));
                        String x=ackreader2.readLine();
                        if (x.contains("ACK OK")) {
                            String[] s1Parts = x.split("#");
                            values.put(KEY_FIELD_DB, s1Parts[0]);
                            values.put(VALUE_FIELD_DB, s1Parts[1]);
                            db2.insert(DhtHelper.TABLE2, null, values);
                            SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();
                            queryBuilder.setTables(DhtHelper.TABLE2);
                            cursor = queryBuilder.query(db2, null, s4,
                                    s3, null, null, sortOrder);
                            is.close();
                        }
                    }catch (Exception e){
                        e.printStackTrace();
                    }
                }
            }
            else{
                if(Keyhash.compareTo(node1.getNodeId())<=0 && Keyhash.compareTo(node1.getPredNodeId())>0){
                    Log.i("In search of node sn2",key+"in node   "+node1.getPortNumber());
                    SQLiteDatabase db = dbHelper.getReadableDatabase();
                    SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();
                    queryBuilder.setTables(DhtHelper.TABLE);
                    cursor = queryBuilder.query(db, null, s4,
                            s3, null, null, sortOrder);
                }
                else if(Keyhash.compareTo(node1.getNodeId())>0 || (Keyhash.compareTo(node1.getPredNodeId())<0 &&Keyhash.compareTo(node1.getNodeId())<0)){
                    Socket is;
                    try{
                        String msg1="QueryKey";
                        Log.i("In search of node sn3",key+"in node   "+node1.getPortNumber()+"sending to succesor node"+node1.getSuccPortNumber());
                        is= new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(node1.getSuccPortNumber()));
                        PrintWriter msgToServer1 = new PrintWriter(is.getOutputStream(), true);
                        msgToServer1.write(msg1+":"+node1.getPortNumber()+":"+key+":"+key);
                        msgToServer1.println();
                        msgToServer1.flush();

                        BufferedReader ackreader2 = new BufferedReader(new InputStreamReader(is.getInputStream()));
                        String x=ackreader2.readLine();
                        if (x.contains("ACK OK")) {
                            String[] s1Parts = x.split("#");
                            values.put(KEY_FIELD_DB, s1Parts[0]);
                            values.put(VALUE_FIELD_DB, s1Parts[1]);
                            db2.insert(DhtHelper.TABLE2, null, values);
                            SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();
                            queryBuilder.setTables(DhtHelper.TABLE2);
                            cursor = queryBuilder.query(db2, null, s4,
                                    s3, null, null, sortOrder);
                            is.close();
                        }
                    }catch (Exception e){
                        e.printStackTrace();
                    }
                }
            }
        }
        return cursor;
        // TODO Auto-generated method stub
        //return null;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }
}
